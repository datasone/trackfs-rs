use std::io::SeekFrom;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

#[derive(Copy, Clone, Debug, Default)]
pub enum WavFormat {
    #[default]
    /// PCM Integer
    Integer,
    /// IEEE 754 Float
    Float,
}

#[derive(Copy, Clone, Default, Debug)]
pub struct WavFormatBlock {
    pub audio_format:    WavFormat,
    pub channels:        u16,
    pub sample_rate:     u32,
    pub bits_per_sample: u16,
}

#[derive(Clone, Default, Debug)]
pub struct WavInfo {
    pub format: WavFormatBlock,
    /// (offset, size)
    pub data:   (u32, u32),
    /// (offset, size)
    pub others: Vec<(u32, u32)>,
}

#[derive(thiserror::Error, Debug)]
pub enum WavParseError {
    #[error("IO error while parsing wav: {0:?}")]
    IO(#[from] tokio::io::Error),
    #[error("invalid RIFF header")]
    InvalidRIFF,
    #[error("invalid wav format chunk")]
    InvalidWavFormat,
    #[error("missing wav format chunk")]
    MissingWavFormat,
    #[error("missing wav data chunk")]
    MissingWavData,
}

impl WavInfo {
    /// This function seek to next block start after read the block size,
    /// returns Ok(None) for invalid wav format block
    pub async fn read_wav(
        mut reader: impl AsyncRead + AsyncSeek + Unpin,
    ) -> Result<Self, WavParseError> {
        let mut buf = [0; 4];
        reader.read_exact(&mut buf).await?;
        if &buf != b"RIFF" {
            return Err(WavParseError::InvalidRIFF);
        }
        reader.read_exact(&mut buf).await?; // Skip file size
        reader.read_exact(&mut buf).await?;
        if &buf != b"WAVE" {
            return Err(WavParseError::InvalidRIFF);
        }

        let mut new_self = Self::default();
        let mut has_format = false;
        let mut has_data = false;

        loop {
            let offset = reader.stream_position().await? as u32;
            let mut buf = [0; 4];
            if let Err(e) = reader.read_exact(&mut buf).await {
                if e.kind() == tokio::io::ErrorKind::UnexpectedEof {
                    break;
                } else {
                    Err(e)?
                }
            }

            if &buf == b"fmt " {
                new_self.format = WavFormatBlock::read_block(&mut reader).await?;
                has_format = true;
            } else if &buf == b"data" {
                reader.read_exact(&mut buf).await?;
                let size = u32::from_le_bytes(buf);
                // RIFF chunks are always at even offsets relative to where they start.
                let size = if size % 2 == 1 { size + 1 } else { size };
                new_self.data = (offset, size);
                has_data = true;
                reader.seek(SeekFrom::Current(size as i64)).await?;
            } else {
                reader.read_exact(&mut buf).await?;
                let size = u32::from_le_bytes(buf);
                // RIFF chunks are always at even offsets relative to where they start.
                let size = if size % 2 == 1 { size + 1 } else { size };
                new_self.others.push((offset, size));
                reader.seek(SeekFrom::Current(size as i64)).await?;
            }
        }
        new_self.others.sort_by_key(|(offset, _)| *offset);

        if !has_format {
            Err(WavParseError::MissingWavFormat)?
        }
        if !has_data {
            Err(WavParseError::MissingWavData)?
        }
        Ok(new_self)
    }

    pub fn to_riff_chunk(&self, data_size: u32) -> [u8; 12] {
        let data_chunk_size = data_size + 8;
        let format_chunk_size = 24;
        let other_size_sum = self.others.iter().map(|(_, size)| size + 8).sum::<u32>();
        let file_size = data_chunk_size + format_chunk_size + other_size_sum + 4;

        let mut header = [0; 12];
        header[0..4].copy_from_slice(b"RIFF");
        header[4..8].copy_from_slice(&file_size.to_le_bytes());
        header[8..12].copy_from_slice(b"WAVE");
        header
    }

    pub fn to_data_header(&self, data_size: u32) -> [u8; 8] {
        let mut header = [0; 8];
        header[0..4].copy_from_slice(b"data");
        header[4..8].copy_from_slice(&data_size.to_le_bytes());
        header
    }

    pub fn total_samples(&self) -> u64 {
        let size = self.data.1;
        size as u64 / self.format.bits_per_sample as u64 * 8 / self.format.channels as u64
    }
}

impl WavFormatBlock {
    pub async fn read_block(mut reader: impl AsyncRead + Unpin) -> Result<Self, WavParseError> {
        let mut buf = [0; 4];
        reader.read_exact(&mut buf).await?;
        if buf != [0x10, 0x00, 0x00, 0x00] {
            return Err(WavParseError::InvalidWavFormat);
        }

        reader.read_exact(&mut buf).await?;
        let audio_format = match buf[0..2] {
            [0x01, 0x00] => WavFormat::Integer,
            [0x03, 0x00] => WavFormat::Float,
            _ => return Err(WavParseError::InvalidWavFormat),
        };
        let channels = buf[2] as u16 + ((buf[3] as u16) << 8);

        reader.read_exact(&mut buf).await?;
        let sample_rate = u32::from_le_bytes(buf);
        reader.read_exact(&mut buf).await?;
        reader.read_exact(&mut buf).await?;
        let bits_per_sample = buf[2] as u16 + ((buf[3] as u16) << 8);

        Ok(Self {
            audio_format,
            channels,
            sample_rate,
            bits_per_sample,
        })
    }

    pub fn into_bytes(self) -> [u8; 24] {
        let bytes_per_block = self.channels * self.bits_per_sample / 8;
        let bytes_per_seconds = bytes_per_block as u32 * self.sample_rate;

        let mut header = Vec::with_capacity(24);
        header.extend(b"fmt ");
        header.extend([0x10, 0x00, 0x00, 0x00]);
        header.extend(match self.audio_format {
            WavFormat::Integer => [0x01, 0x00],
            WavFormat::Float => [0x03, 0x00],
        });
        header.extend(self.channels.to_le_bytes());
        header.extend(self.sample_rate.to_le_bytes());
        header.extend(bytes_per_seconds.to_le_bytes());
        header.extend(bytes_per_block.to_le_bytes());
        header.extend(self.bits_per_sample.to_le_bytes());
        header.try_into().unwrap()
    }
}
