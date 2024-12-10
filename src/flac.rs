mod crc;

use std::{
    collections::HashMap,
    convert::TryFrom,
    io::{Cursor, Read, Seek, SeekFrom, Write},
    str::Utf8Error,
};

use image::{ColorType, DynamicImage, ImageFormat};
use num_enum::TryFromPrimitive;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::utils::{InvalidPointerError, SliceReadExt};

#[derive(Error, Debug)]
pub enum FlacParseError {
    #[error("early end on parsed bytes")]
    EarlyEnd,
    #[error("invalid or reserved metadata block type {0}")]
    InvalidMetadataBlockType(u8),
    #[error("invalid picture type {0}")]
    InvalidPictureType(u8),
    #[error("invalid vorbis comment encoding: {0}")]
    InvalidVorbisCommentEncoding(#[from] Utf8Error),
    #[error("invalid vorbis comment: {0}")]
    InvalidVorbisComment(String),
    #[error("invalid frame header: {0:?}")]
    InvalidFrameHeader(Vec<u8>),
    #[error("invalid frame position: {0:?}")]
    InvalidFramePosition(Vec<u8>),
    #[error("I/O error {0}")]
    IO(#[from] std::io::Error),
}

impl From<InvalidPointerError> for FlacParseError {
    fn from(_value: InvalidPointerError) -> Self {
        Self::EarlyEnd
    }
}

#[derive(Clone, Debug, Default)]
pub struct FlacMetadataBlock {
    pub is_last:    bool,
    pub block_type: FlacMetadataBlockType,
    pub content:    FlacMetadataBlockContent,
}

impl FlacMetadataBlock {
    pub async fn read_block(mut reader: impl AsyncRead + Unpin) -> Result<Self, FlacParseError> {
        let mut buf = [0; 4];
        reader.read_exact(&mut buf[0..1]).await?;

        let type_byte = buf[0];
        let last_metadata = (type_byte & (1 << 7)) != 0;

        let type_byte = type_byte & ((1 << 7) - 1);
        let block_type = FlacMetadataBlockType::try_from(type_byte)
            .map_err(|_| FlacParseError::InvalidMetadataBlockType(type_byte))?;

        reader.read_exact(&mut buf[1..]).await?;
        buf[0] = 0;
        let block_size = u32::from_be_bytes(buf);

        let mut content_buf = vec![0; block_size as usize];
        reader.read_exact(&mut content_buf).await?;

        let content = FlacMetadataBlockContent::parse_bytes(&content_buf, block_type)?;
        Ok(Self {
            is_last: last_metadata,
            block_type,
            content,
        })
    }

    #[allow(dead_code)]
    /// Expected to return `Ok(is_last_metadata_block)`
    fn skip_block(mut reader: impl Read + Seek) -> Result<bool, FlacParseError> {
        let mut buf = [0; 4];
        reader.read_exact(&mut buf[0..1])?;

        let last_metadata = (buf[0] & (1 << 7)) != 0;
        reader.read_exact(&mut buf[1..])?;
        buf[0] = 0;

        let block_size = u32::from_be_bytes(buf);
        reader.seek(SeekFrom::Current(block_size as _))?;

        Ok(last_metadata)
    }

    pub fn write_block(&self, mut writer: impl Write) -> std::io::Result<()> {
        let type_byte = if self.is_last { 1 << 7 } else { 0 };
        let type_byte = type_byte | self.block_type as u8;

        let content = self.content.to_bytes();
        let mut length = (content.len() as u32).to_be_bytes();
        length[0] = type_byte;

        writer.write_all(&length)?;
        writer.write_all(&content)?;

        Ok(())
    }
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, TryFromPrimitive, Debug, Default)]
pub enum FlacMetadataBlockType {
    #[default]
    StreamInfo    = 0,
    Padding       = 1,
    Application   = 2,
    SeekTable     = 3,
    VorbisComment = 4,
    CUESheet      = 5,
    Picture       = 6,
}

/// The FLAC-embedded CUESheet metadata block **is not** used to parse
/// information, as it only allows TOC data, not textual data. However, the
/// `cuesheet` tag in vorbis comment block will be processed.
#[derive(Clone, Debug)]
pub enum FlacMetadataBlockContent {
    StreamInfo(StreamInfoBlock),
    VorbisComment(VorbisCommentBlock),
    Picture(PictureBlock),
    Others(Vec<u8>),
}

impl Default for FlacMetadataBlockContent {
    fn default() -> Self {
        Self::Others(Vec::new())
    }
}

impl FlacMetadataBlockContent {
    pub fn parse_bytes(
        bytes: &[u8],
        block_type: FlacMetadataBlockType,
    ) -> Result<Self, FlacParseError> {
        Ok(match block_type {
            FlacMetadataBlockType::StreamInfo => {
                let mut pointer = 0;

                let min_block_size = bytes.read_u16(&mut pointer)?;
                let max_block_size = bytes.read_u16(&mut pointer)?;
                let min_frame_size = bytes.read_u24(&mut pointer)?;
                let max_frame_size = bytes.read_u24(&mut pointer)?;
                let chunk = bytes.read_bytes(&mut pointer, 4)?;
                let sample_rate = ((chunk[0] as u32) << (4 + 8))
                    + ((chunk[1] as u32) << 4)
                    + (chunk[2] as u32 >> 4);
                let channels = (chunk[2] & 0b1110) >> 1;
                let bits_per_sample = (chunk[2] & 0b1) * 0x10 + (chunk[3] >> 4);
                let sample_count =
                    (((chunk[3] & 0b1111) as u64) << 32) + bytes.read_u32(&mut pointer)? as u64;
                let md5 = bytes.read_bytes(&mut pointer, 16)?;

                FlacMetadataBlockContent::StreamInfo(StreamInfoBlock {
                    min_block_size,
                    max_block_size,
                    min_frame_size,
                    max_frame_size,
                    sample_rate,
                    channels,
                    bits_per_sample,
                    sample_count,
                    md5: md5.try_into().unwrap(),
                })
            }
            FlacMetadataBlockType::VorbisComment => {
                let mut pointer = 0;

                let vendor_len = bytes.read_u32_le(&mut pointer)? as usize;
                let vendor_bytes = bytes.read_bytes(&mut pointer, vendor_len)?;
                let vendor = std::str::from_utf8(vendor_bytes)?.to_owned();

                let user_comments_len = bytes.read_u32_le(&mut pointer)? as usize;
                let user_comments = (0..user_comments_len)
                    .map(|_| {
                        let comment_len = bytes.read_u32_le(&mut pointer)? as usize;
                        let comment_bytes = bytes.read_bytes(&mut pointer, comment_len)?;

                        let comment_str = std::str::from_utf8(comment_bytes)?;
                        let split = comment_str.split_once('=');

                        match split {
                            Some((key, value)) => Ok((key.to_owned(), value.to_owned())),
                            None => {
                                Err(FlacParseError::InvalidVorbisComment(comment_str.to_owned()))
                            }
                        }
                    })
                    .collect::<Result<HashMap<_, _>, FlacParseError>>()?;

                Self::VorbisComment(VorbisCommentBlock {
                    vendor,
                    user_comments,
                })
            }
            FlacMetadataBlockType::Picture => {
                let mut pointer = 0;

                let picture_type = bytes.read_u32(&mut pointer)? as u8;
                let picture_type = PictureType::try_from(picture_type)
                    .map_err(|_| FlacParseError::InvalidPictureType(picture_type))?;

                let mime_len = bytes.read_u32(&mut pointer)? as usize;
                let mime_type = bytes.read_bytes(&mut pointer, mime_len)?;
                let mime_type = std::str::from_utf8(mime_type)?;

                let desc_len = bytes.read_u32(&mut pointer)? as usize;
                let description = bytes.read_bytes(&mut pointer, desc_len)?;
                let description = std::str::from_utf8(description)?;

                let width = bytes.read_u32(&mut pointer)?;
                let height = bytes.read_u32(&mut pointer)?;
                let color_depth = bytes.read_u32(&mut pointer)?;
                let colors = bytes.read_u32(&mut pointer)?;

                let data_len = bytes.read_u32(&mut pointer)? as usize;
                let data = bytes.read_bytes(&mut pointer, data_len)?;

                Self::Picture(PictureBlock {
                    picture_type,
                    mime_type: mime_type.to_owned(),
                    description: description.to_owned(),
                    width,
                    height,
                    color_depth,
                    colors,
                    picture_data: data.to_vec(),
                })
            }
            _ => Self::Others(bytes.to_vec()),
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::StreamInfo(stream_info) => {
                let mut out = Vec::with_capacity(34);
                out.extend(stream_info.min_block_size.to_be_bytes());
                out.extend(stream_info.max_block_size.to_be_bytes());
                out.extend(&stream_info.min_frame_size.to_be_bytes()[1..]);
                out.extend(&stream_info.max_frame_size.to_be_bytes()[1..]);
                let mut chunk = ((stream_info.sample_rate) << 12).to_be_bytes();
                chunk[2] |= stream_info.channels << 1;
                chunk[2] |= stream_info.bits_per_sample >> 4;
                chunk[3] = ((stream_info.bits_per_sample & 0b1111) << 4)
                    | (stream_info.sample_count >> 32) as u8;
                out.extend(chunk);
                out.extend((stream_info.sample_count as u32).to_be_bytes());
                out.extend(stream_info.md5);
                out
            }
            Self::VorbisComment(vorbis_comment) => {
                let mut vec = vec![];

                let vendor_bytes = vorbis_comment.vendor.as_bytes();
                let vendor_len = (vendor_bytes.len() as u32).to_le_bytes();

                vec.extend_from_slice(&vendor_len);
                vec.extend_from_slice(vendor_bytes);

                let user_comments_len = (vorbis_comment.user_comments.len() as u32).to_le_bytes();
                vec.extend_from_slice(&user_comments_len);

                for (key, value) in vorbis_comment.user_comments.iter() {
                    let comment = format!("{}={}", key, value);

                    let comment_bytes = comment.into_bytes();
                    let length = (comment_bytes.len() as u32).to_le_bytes();

                    vec.extend_from_slice(&length);
                    vec.extend_from_slice(&comment_bytes);
                }

                vec
            }
            Self::Picture(picture) => {
                let mut vec = vec![];

                let picture_type = (picture.picture_type as u32).to_be_bytes();
                vec.extend_from_slice(&picture_type);

                let mime_bytes = picture.mime_type.as_bytes();
                let mime_len = (mime_bytes.len() as u32).to_be_bytes();
                vec.extend_from_slice(&mime_len);
                vec.extend_from_slice(mime_bytes);

                let desc_bytes = picture.description.as_bytes();
                let desc_len = (desc_bytes.len() as u32).to_be_bytes();
                vec.extend_from_slice(&desc_len);
                vec.extend_from_slice(desc_bytes);

                vec.extend_from_slice(&picture.width.to_be_bytes());
                vec.extend_from_slice(&picture.height.to_be_bytes());
                vec.extend_from_slice(&picture.color_depth.to_be_bytes());
                vec.extend_from_slice(&picture.colors.to_be_bytes());

                let data_len = (picture.picture_data.len() as u32).to_be_bytes();
                vec.extend_from_slice(&data_len);
                vec.extend_from_slice(&picture.picture_data);

                vec
            }
            Self::Others(bytes) => (*bytes).to_owned(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct StreamInfoBlock {
    pub min_block_size: u16,
    pub max_block_size: u16,
    pub min_frame_size: u32,
    pub max_frame_size: u32,
    sample_rate:        u32,
    /// Value stored in FLAC, equals to (number of channels) - 1
    channels:           u8,
    /// Value stored in FLAC, equals to (bits per sample) - 1
    bits_per_sample:    u8,
    pub sample_count:   u64,
    pub md5:            [u8; 16],
}

impl StreamInfoBlock {
    pub fn get_sample_rate(&self) -> u32 {
        self.sample_rate
    }

    pub fn get_channels(&self) -> u8 {
        self.channels + 1
    }

    pub fn get_bits(&self) -> u8 {
        self.bits_per_sample + 1
    }
}

#[derive(Clone, Debug)]
pub struct VorbisCommentBlock {
    vendor:            String,
    pub user_comments: HashMap<String, String>,
}

impl VorbisCommentBlock {
    pub fn new() -> Self {
        Self {
            vendor:        "trackfs-rs".to_string(),
            user_comments: Default::default(),
        }
    }

    pub fn add_vorbis_comment(&mut self, key: impl ToString, value: impl ToString) {
        let key = key.to_string();
        let value = value.to_string();

        let key = self
            .user_comments
            .keys()
            .find(|k| k.eq_ignore_ascii_case(&key))
            .cloned()
            .unwrap_or(key);

        self.user_comments.insert(key, value);
    }

    pub fn get(&self, key: &String) -> Option<&String> {
        self.user_comments.get(key)
    }
}

#[derive(Clone, Debug)]
pub struct PictureBlock {
    picture_type: PictureType,
    mime_type:    String,
    description:  String,
    width:        u32,
    height:       u32,
    color_depth:  u32,
    /// For indexed-color pictures (e.g. GIF), the number of colors used, or 0
    /// for non-indexed pictures.
    colors:       u32,
    picture_data: Vec<u8>,
}

impl PictureBlock {
    #[allow(dead_code)]
    fn from_dynamic_image(
        picture_type: PictureType,
        fmt: ImageFormat,
        img: &DynamicImage,
        data: Vec<u8>,
    ) -> Self {
        Self {
            picture_type,
            mime_type: pic_fmt_to_mime(fmt),
            description: "".to_owned(),
            width: img.width(),
            height: img.height(),
            color_depth: color_type_to_depth(img.color()),
            colors: 0,
            picture_data: data,
        }
    }
}

fn pic_fmt_to_mime(fmt: ImageFormat) -> String {
    match fmt {
        ImageFormat::Png => "image/png".to_owned(),
        ImageFormat::Jpeg => "image/jpeg".to_owned(),
        ImageFormat::Gif => "image/gif".to_owned(),
        ImageFormat::WebP => "image/webp".to_owned(),
        ImageFormat::Tiff => "image/tiff".to_owned(),
        ImageFormat::Bmp => "image/bmp".to_owned(),
        ImageFormat::Ico => "image/vnd.microsoft.icon".to_owned(),
        ImageFormat::Avif => "image/avif".to_owned(),
        _ => unimplemented!(),
    }
}

fn color_type_to_depth(color_type: ColorType) -> u32 {
    match color_type {
        ColorType::L8 => 8,
        ColorType::La8 => 8 * 2,
        ColorType::Rgb8 => 8 * 3,
        ColorType::Rgba8 => 8 * 4,
        ColorType::L16 => 16,
        ColorType::La16 => 16 * 2,
        ColorType::Rgb16 => 16 * 3,
        ColorType::Rgba16 => 16 * 4,
        ColorType::Rgb32F => 32 * 3,
        ColorType::Rgba32F => 32 * 4,
        _ => unimplemented!(),
    }
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, TryFromPrimitive, Debug)]
pub enum PictureType {
    Other              = 0,
    /// 32x32 pixels 'file icon' (PNG only)
    FileIcon           = 1,
    OtherFileIcon      = 2,
    FrontCover         = 3,
    BackCover          = 4,
    LeafletPage        = 5,
    /// e.g. label side of CD
    Media              = 6,
    /// Lead artist/lead performer/soloist
    LeadArtist         = 7,
    Artist             = 8,
    Conductor          = 9,
    /// Band/Orchestra
    Band               = 10,
    Composer           = 11,
    Lyricist           = 12,
    RecordingLocation  = 13,
    DuringRecording    = 14,
    DuringPerformance  = 15,
    Movie              = 16,
    BrightColouredFish = 17,
    Illustration       = 18,
    ArtistLogoType     = 19,
    StudioLogoType     = 20,
}

#[derive(Clone, Debug)]
pub struct FlacFrame {
    pub metadata: FlacFrameMetadata,
    frame_data:   Vec<u8>,
}

impl FlacFrame {
    /// Currently, this tool don't check the CRC values.
    pub fn read_frame(
        reader: impl Read,
        stream_info: &StreamInfoBlock,
        size: usize,
    ) -> Result<Self, FlacParseError> {
        let mut frame_data = Vec::with_capacity(size);
        reader.take(size as u64).read_to_end(&mut frame_data)?;

        let (metadata, header_size) = FlacFrameMetadata::read(&*frame_data, stream_info)?;
        let frame_data = frame_data[header_size + 1..frame_data.len() - 2].to_vec(); // Dropping CRC8 in header and CRC16 in footer

        Ok(Self {
            metadata,
            frame_data,
        })
    }

    /// Returns the sample offset of each frame
    /// For fixed block size, frame number is also converted
    pub fn scan_frames(
        mut reader: impl Read + Seek,
        frame_sizes: impl IntoIterator<Item = u64>,
        stream_info: &StreamInfoBlock,
    ) -> Result<Vec<u64>, FlacParseError> {
        let mut block_size = None;

        frame_sizes
            .into_iter()
            .map(|size| {
                let (metadata, bytes) = FlacFrameMetadata::read(&mut reader, stream_info)?;
                reader.seek(SeekFrom::Current(size as i64 - bytes as i64))?;
                Ok(match metadata.position {
                    FlacFramePosition::SampleCount(samples) => samples,
                    FlacFramePosition::FrameCount(frames) => {
                        // The last frame will have smaller block size even for
                        // fixed block size stream, we need to handle that
                        if block_size.is_none() {
                            block_size = Some(metadata.block_size.get_size());
                        }
                        frames as u64 * block_size.unwrap() as u64
                    }
                })
            })
            .collect()
    }

    #[allow(dead_code)]
    fn extract_frames(data: &[u8]) -> Result<&[u8], FlacParseError> {
        let mut cursor = Cursor::new(data);
        cursor.seek_relative(4)?;

        loop {
            let is_last = FlacMetadataBlock::skip_block(&mut cursor)?;
            if is_last {
                break;
            }
        }

        Ok(&data[cursor.position() as usize..])
    }

    pub fn into_bytes(self) -> Vec<u8> {
        let mut header_bytes = self.metadata.to_bytes();
        header_bytes.extend(self.frame_data);
        let crc16 = crc::crc16(&header_bytes);
        header_bytes.extend(crc16.to_be_bytes());

        header_bytes
    }
}

#[derive(Clone, Debug)]
pub struct FlacFrameMetadata {
    pub blocking_strategy:  FlacBlockingStrategy,
    pub block_size:         FlacBlockSize,
    pub sample_rate:        FlacSampleRate,
    pub channel_assignment: FlacChannelAssignment,
    pub sample_bits:        FlacSampleBits,
    pub position:           FlacFramePosition,
}

impl FlacFrameMetadata {
    /// This function expect to read from start of the frame (sync code
    /// identified), but the reader should not consume the sync code bytes
    pub fn read(
        mut reader: impl Read,
        stream_info: &StreamInfoBlock,
    ) -> Result<(Self, usize), FlacParseError> {
        let mut bytes_read = 4;

        let mut header_bytes = [0; 4];
        reader.read_exact(&mut header_bytes)?;

        macro_rules! header_err {
            () => {
                |_| FlacParseError::InvalidFrameHeader(header_bytes.to_vec())
            };
        }

        let blocking_strategy =
            FlacBlockingStrategy::try_from(header_bytes[1] & 0b1).map_err(header_err!())?;
        let block_size =
            FlacBlockSizeType::try_from((header_bytes[2] & 0xF0) >> 4).map_err(header_err!())?;
        if block_size == FlacBlockSizeType::Reserved {
            Err(FlacParseError::InvalidFrameHeader(header_bytes.to_vec()))?;
        }
        let sample_rate =
            FlacSampleRateType::try_from(header_bytes[2] & 0x0F).map_err(header_err!())?;
        if sample_rate == FlacSampleRateType::Invalid {
            Err(FlacParseError::InvalidFrameHeader(header_bytes.to_vec()))?;
        }
        let channel_assignment = (header_bytes[3] & 0xF0) >> 4;
        if channel_assignment >= 0b1011 {
            Err(FlacParseError::InvalidFrameHeader(header_bytes.to_vec()))?;
        }
        let channel_assignment =
            FlacChannelAssignment::try_from(channel_assignment).map_err(header_err!())?;
        let sample_bits =
            FlacSampleBitsType::try_from((header_bytes[3] & 0b1110) >> 1).map_err(header_err!())?;
        let sample_bits = if sample_bits == FlacSampleBitsType::FromStreamInfo {
            FlacSampleBits::Dynamic(
                FlacSampleBitsType::FromStreamInfo,
                stream_info.bits_per_sample,
            )
        } else if sample_bits == FlacSampleBitsType::Reserved {
            return Err(FlacParseError::InvalidFrameHeader(header_bytes.to_vec()));
        } else {
            FlacSampleBits::PreDefined(sample_bits)
        };

        let (position, bytes) = Self::read_utf8_digits(&mut reader)?;
        let position = match blocking_strategy {
            FlacBlockingStrategy::Fixed => FlacFramePosition::FrameCount(position as u32),
            FlacBlockingStrategy::Variable => FlacFramePosition::SampleCount(position),
        };
        bytes_read += bytes;

        let mut buf = [0; 2];

        let block_size = if block_size == FlacBlockSizeType::HeaderEndU8 {
            bytes_read += 1;
            reader.read_exact(&mut buf[0..1])?;
            FlacBlockSize::Dynamic(FlacBlockSizeType::HeaderEndU8, buf[0] as u16)
        } else if block_size == FlacBlockSizeType::HeaderEndU16 {
            bytes_read += 2;
            reader.read_exact(&mut buf)?;
            FlacBlockSize::Dynamic(FlacBlockSizeType::HeaderEndU16, u16::from_be_bytes(buf))
        } else {
            FlacBlockSize::PreDefined(block_size)
        };

        let sample_rate = match sample_rate {
            FlacSampleRateType::FromStreamInfo => {
                FlacSampleRate::Dynamic(FlacSampleRateType::FromStreamInfo, stream_info.sample_rate)
            }
            FlacSampleRateType::HeaderEndU8KHz => {
                bytes_read += 1;
                reader.read_exact(&mut buf[0..1])?;
                FlacSampleRate::Dynamic(FlacSampleRateType::HeaderEndU8KHz, buf[0] as u32)
            }
            sr_type @ (FlacSampleRateType::HeaderEndU16Hz
            | FlacSampleRateType::HeaderEndU16TenHz) => {
                bytes_read += 2;
                reader.read_exact(&mut buf)?;
                FlacSampleRate::Dynamic(sr_type, u16::from_be_bytes(buf) as u32)
            }
            _ => FlacSampleRate::PreDefined(sample_rate),
        };

        let metadata = Self {
            blocking_strategy,
            block_size,
            sample_rate,
            channel_assignment,
            sample_bits,
            position,
        };

        Ok((metadata, bytes_read))
    }

    /// Returns `(decoded number, code bytes)`
    fn read_utf8_digits(mut reader: impl Read) -> Result<(u64, usize), FlacParseError> {
        let mut buf = [0; 7];
        reader.read_exact(&mut buf[0..1])?;
        let (ones, _) = (0..8).rev().fold((0, false), |(count, stop), i| {
            if !stop && (buf[0] >> i) & 1 == 1 {
                (count + 1, false)
            } else {
                (count, true)
            }
        });
        let bytes_to_read = if ones > 0 { ones - 1 } else { 0 };
        reader.read_exact(&mut buf[1..1 + bytes_to_read])?;

        let bits = match bytes_to_read {
            0 => 7,
            1 => 11,
            2 => 16,
            3 => 21,
            4 => 26,
            5 => 31,
            6 => 36,
            _ => unreachable!(),
        };
        let initial =
            ((buf[0] as u64) & (!(0xFF << (bits - bytes_to_read * 6)))) << (bytes_to_read * 6);

        let number =
            buf[1..1 + bytes_to_read]
                .iter()
                .enumerate()
                .try_fold(initial, |res, (i, byte)| {
                    if *byte >> 6 != 0b10 {
                        Err(FlacParseError::InvalidFramePosition(
                            buf[0..1 + bytes_to_read].to_vec(),
                        ))
                    } else {
                        Ok(res | (((byte & 0b00111111) as u64) << ((bytes_to_read - i - 1) * 6)))
                    }
                });

        number.map(|number| (number, 1 + bytes_to_read))
    }

    fn encode_utf8_digits(number: u64) -> Vec<u8> {
        let bits = u64::BITS - number.leading_zeros();
        let encoded_bytes = match bits {
            0..=7 => 1,
            8..=11 => 2,
            12..=16 => 3,
            17..=21 => 4,
            22..=26 => 5,
            27..=31 => 6,
            32..=36 => 7,
            _ => unreachable!(),
        };

        match encoded_bytes {
            1 => vec![number as u8],
            _ => {
                let mut out_bytes = vec![0; encoded_bytes as usize];

                let mut number = number;
                for i in (1..encoded_bytes as usize).rev() {
                    out_bytes[i] = 0b10000000u8 | (number as u8 & 0b00111111);
                    number >>= 6;
                }

                let leading_byte = 0xFFu8 << (8 - encoded_bytes);
                let leading_byte = leading_byte | number as u8;
                out_bytes[0] = leading_byte;

                out_bytes
            }
        }
    }

    /// Metadata to header bytes, with CRC8 value
    fn to_bytes(&self) -> Vec<u8> {
        let mut out = vec![];
        let syncing_bytes = match self.blocking_strategy {
            FlacBlockingStrategy::Fixed => [0xFF, 0xF8],
            FlacBlockingStrategy::Variable => [0xFF, 0xF9],
        };
        let bs_sr = ((self.block_size.get_type() as u8) << 4) + self.sample_rate.get_type() as u8;
        let ch_bits =
            ((self.channel_assignment as u8) << 4) + ((self.sample_bits.get_type() as u8) << 1);

        let position = Self::encode_utf8_digits(self.position.to_u64());

        out.extend(syncing_bytes);
        out.push(bs_sr);
        out.push(ch_bits);
        out.extend(position);

        match self.block_size.get_type() {
            FlacBlockSizeType::HeaderEndU8 => out.push(self.block_size.get_size() as u8),
            FlacBlockSizeType::HeaderEndU16 => out.extend(self.block_size.get_size().to_be_bytes()),
            _ => {}
        }

        match self.sample_rate.get_type() {
            FlacSampleRateType::HeaderEndU8KHz => out.push(self.sample_rate.get_sr_raw() as u8),
            FlacSampleRateType::HeaderEndU16Hz | FlacSampleRateType::HeaderEndU16TenHz => {
                out.extend(self.sample_rate.get_sr_raw().to_be_bytes())
            }
            _ => {}
        }

        let crc = crc::crc8(&out);
        out.push(crc);

        out
    }
}
#[repr(u8)]
#[derive(Clone, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum FlacBlockingStrategy {
    Fixed    = 0,
    Variable = 1,
}

#[derive(Clone, Debug)]
pub enum FlacBlockSize {
    PreDefined(FlacBlockSizeType),
    Dynamic(FlacBlockSizeType, u16),
}

impl FlacBlockSize {
    #[allow(dead_code)]
    pub fn new(size: u16) -> Self {
        match size {
            192 => Self::PreDefined(FlacBlockSizeType::Samples192),
            576 => Self::PreDefined(FlacBlockSizeType::Samples576),
            1152 => Self::PreDefined(FlacBlockSizeType::Samples1152),
            2304 => Self::PreDefined(FlacBlockSizeType::Samples2304),
            4608 => Self::PreDefined(FlacBlockSizeType::Samples4608),
            256 => Self::PreDefined(FlacBlockSizeType::Samples256),
            512 => Self::PreDefined(FlacBlockSizeType::Samples512),
            1024 => Self::PreDefined(FlacBlockSizeType::Samples1024),
            2048 => Self::PreDefined(FlacBlockSizeType::Samples2048),
            4096 => Self::PreDefined(FlacBlockSizeType::Samples4096),
            8192 => Self::PreDefined(FlacBlockSizeType::Samples8192),
            16384 => Self::PreDefined(FlacBlockSizeType::Samples16384),
            32768 => Self::PreDefined(FlacBlockSizeType::Samples32768),
            ..256 => Self::Dynamic(FlacBlockSizeType::HeaderEndU8, size),
            _ => Self::Dynamic(FlacBlockSizeType::HeaderEndU16, size),
        }
    }

    fn get_type(&self) -> FlacBlockSizeType {
        match self {
            FlacBlockSize::PreDefined(t) => *t,
            FlacBlockSize::Dynamic(t, _) => *t,
        }
    }

    pub fn get_size(&self) -> u16 {
        match self {
            FlacBlockSize::PreDefined(t) => t.get_size().unwrap(),
            FlacBlockSize::Dynamic(_, s) => *s,
        }
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum FlacBlockSizeType {
    Reserved     = 0,
    Samples192   = 1,
    Samples576   = 2,
    Samples1152  = 3,
    Samples2304  = 4,
    Samples4608  = 5,
    HeaderEndU8  = 6,
    HeaderEndU16 = 7,
    Samples256   = 8,
    Samples512   = 9,
    Samples1024  = 10,
    Samples2048  = 11,
    Samples4096  = 12,
    Samples8192  = 13,
    Samples16384 = 14,
    Samples32768 = 15,
}

impl FlacBlockSizeType {
    pub fn get_size(&self) -> Option<u16> {
        match self {
            FlacBlockSizeType::Reserved => None,
            FlacBlockSizeType::Samples192 => Some(192),
            FlacBlockSizeType::Samples576 => Some(576),
            FlacBlockSizeType::Samples1152 => Some(1152),
            FlacBlockSizeType::Samples2304 => Some(2304),
            FlacBlockSizeType::Samples4608 => Some(4608),
            FlacBlockSizeType::HeaderEndU8 => None,
            FlacBlockSizeType::HeaderEndU16 => None,
            FlacBlockSizeType::Samples256 => Some(256),
            FlacBlockSizeType::Samples512 => Some(512),
            FlacBlockSizeType::Samples1024 => Some(1024),
            FlacBlockSizeType::Samples2048 => Some(2048),
            FlacBlockSizeType::Samples4096 => Some(4096),
            FlacBlockSizeType::Samples8192 => Some(8192),
            FlacBlockSizeType::Samples16384 => Some(16384),
            FlacBlockSizeType::Samples32768 => Some(32768),
        }
    }
}

#[derive(Clone, Debug)]
pub enum FlacSampleRate {
    PreDefined(FlacSampleRateType),
    /// Stores the raw value
    Dynamic(FlacSampleRateType, u32),
}

impl FlacSampleRate {
    fn get_type(&self) -> FlacSampleRateType {
        match self {
            FlacSampleRate::PreDefined(t) => *t,
            FlacSampleRate::Dynamic(t, _) => *t,
        }
    }

    /// This returns raw stored data **instead of Hz**, only for Dynamic sample
    /// rate
    fn get_sr_raw(&self) -> u32 {
        match self {
            FlacSampleRate::PreDefined(_) => unreachable!(),
            FlacSampleRate::Dynamic(_, sr) => *sr,
        }
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum FlacSampleRateType {
    FromStreamInfo    = 0,
    SR88_2K           = 1,
    SR176_4K          = 2,
    SR192K            = 3,
    SR8K              = 4,
    SR16K             = 5,
    SR22_05K          = 6,
    SR24K             = 7,
    SR32K             = 8,
    SR44_1K           = 9,
    SR48K             = 10,
    SR96K             = 11,
    HeaderEndU8KHz    = 12,
    HeaderEndU16Hz    = 13,
    HeaderEndU16TenHz = 14,
    Invalid           = 15,
}

impl FlacSampleRateType {
    #[allow(dead_code)]
    pub fn get_sample_rate_hz(&self) -> Option<u32> {
        match self {
            FlacSampleRateType::FromStreamInfo => None,
            FlacSampleRateType::SR88_2K => Some(88_200),
            FlacSampleRateType::SR176_4K => Some(176_400),
            FlacSampleRateType::SR192K => Some(192_000),
            FlacSampleRateType::SR8K => Some(8_000),
            FlacSampleRateType::SR16K => Some(16_000),
            FlacSampleRateType::SR22_05K => Some(22_050),
            FlacSampleRateType::SR24K => Some(24_000),
            FlacSampleRateType::SR32K => Some(32_000),
            FlacSampleRateType::SR44_1K => Some(44_100),
            FlacSampleRateType::SR48K => Some(48_000),
            FlacSampleRateType::SR96K => Some(96_000),
            FlacSampleRateType::HeaderEndU8KHz => None,
            FlacSampleRateType::HeaderEndU16Hz => None,
            FlacSampleRateType::HeaderEndU16TenHz => None,
            FlacSampleRateType::Invalid => None,
        }
    }
}
#[repr(u8)]
#[derive(Copy, Clone, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum FlacChannelAssignment {
    Mono            = 0,
    LeftRight       = 1,
    LeftRightCenter = 2,
    FrontLeftRightBackLeftRight = 3,
    FrontLeftRightCenterBackLeftRight = 4,
    FrontLeftRightCenterLFEBackLeftRight = 5,
    FrontLeftRightCenterLFEBackCenterSideLeftRight = 6,
    FrontLeftRightCenterLFEBackLeftRightSideLeftRight = 7,
    LeftSideStereo  = 8,
    RightSideStereo = 9,
    MidSide         = 10,
}

#[derive(Clone, Debug)]
pub enum FlacSampleBits {
    PreDefined(FlacSampleBitsType),
    Dynamic(FlacSampleBitsType, #[allow(dead_code)] u8),
}

impl FlacSampleBits {
    pub fn get_type(&self) -> FlacSampleBitsType {
        match self {
            FlacSampleBits::PreDefined(t) => *t,
            FlacSampleBits::Dynamic(t, _) => *t,
        }
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, Eq, PartialEq, TryFromPrimitive)]
pub enum FlacSampleBitsType {
    FromStreamInfo = 0,
    Bits8          = 1,
    Bits12         = 2,
    Reserved       = 3,
    Bits16         = 4,
    Bits20         = 5,
    Bits24         = 6,
    Bits32         = 7,
}

#[derive(Clone, Debug)]
pub enum FlacFramePosition {
    /// 36-bit Sample Number
    SampleCount(u64),
    /// 31-bit Frame Number
    FrameCount(u32),
}

impl FlacFramePosition {
    pub fn to_u64(&self) -> u64 {
        match self {
            FlacFramePosition::SampleCount(n) => *n,
            FlacFramePosition::FrameCount(n) => *n as _,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_utf8_digits() {
        let bytes_1 = [0b01001001u8];
        let bytes_3 = [0b11100001u8, 0b10101101, 0b10000101];
        let bytes_7 = [
            0b11111110u8,
            0b10001011,
            0b10110101,
            0b10001111,
            0b10101101,
            0b10110101,
            0b10101101,
        ];
        let bytes_invalid = [0b11100001u8, 0b11101101, 0b01000101];

        let number_1 = 0b1001001;
        let number_3 = 0b0001_101101_000101;
        let number_7 = 0b001011_110101_001111_101101_110101_101101;

        assert_eq!(
            FlacFrameMetadata::read_utf8_digits(&bytes_1[..]).unwrap().0,
            number_1
        );
        assert_eq!(
            FlacFrameMetadata::read_utf8_digits(&bytes_3[..]).unwrap().0,
            number_3
        );
        assert_eq!(
            FlacFrameMetadata::read_utf8_digits(&bytes_7[..]).unwrap().0,
            number_7
        );
        assert!(matches!(
            FlacFrameMetadata::read_utf8_digits(&bytes_invalid[..]),
            Err(FlacParseError::InvalidFramePosition(_))
        ));
    }

    #[test]
    fn test_encode_utf8_digits() {
        let bytes_1 = [0b01001001u8];
        let bytes_3 = [0b11100001u8, 0b10101101, 0b10000101];
        let bytes_7 = [
            0b11111110u8,
            0b10001011,
            0b10110101,
            0b10001111,
            0b10101101,
            0b10110101,
            0b10101101,
        ];

        let number_1 = 0b1001001;
        let number_3 = 0b0001_101101_000101;
        let number_7 = 0b001011_110101_001111_101101_110101_101101;

        assert_eq!(FlacFrameMetadata::encode_utf8_digits(number_1), bytes_1);
        assert_eq!(FlacFrameMetadata::encode_utf8_digits(number_3), bytes_3);
        assert_eq!(FlacFrameMetadata::encode_utf8_digits(number_7), bytes_7);
    }
}
