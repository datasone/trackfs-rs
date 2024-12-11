use std::{
    ffi::OsStr,
    io::{Cursor, Read, Seek, SeekFrom, Write},
    iter::zip,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::anyhow;
use cue_rw::CUEFile;
use futures::stream::StreamExt;
use itertools::Itertools;
use num_rational::{Rational32, Rational64};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt};

use crate::{
    flac,
    flac::{
        FlacBlockingStrategy, FlacFrame, FlacFramePosition, FlacMetadataBlock,
        FlacMetadataBlockContent, FlacMetadataBlockType, FlacParseError, StreamInfoBlock,
        VorbisCommentBlock,
    },
    fs::LibFlacDecEnc,
    libflac_wrapper::{FlacDecoder, FlacEncoder, FlacFrameData, SeekableRead},
    wav::WavInfo,
};

#[derive(Clone, Debug)]
pub struct TrackInfo {
    pub track_id:   usize,
    /// The start position of samples in this file
    pub sample_pos: u64,
}

#[derive(Clone, Debug)]
pub struct CUEInfo {
    pub cue_name:          PathBuf,
    pub passthrough_files: Vec<FileInfo>,
    pub files_info:        Vec<FileInfo>,
}

#[derive(Clone, Debug)]
pub struct FileInfo {
    pub file_path:       PathBuf,
    pub cue:             Arc<CUEFile>,
    pub tracks_info:     Vec<TrackInfo>,
    pub sample_rate:     u32,
    pub total_samples:   u64,
    pub channels:        u8,
    pub bits_per_sample: u8,
    pub wav_info:        Option<WavInfo>,
}

pub async fn process_flac_embedded_cue(
    flac_path: impl AsRef<Path>,
) -> anyhow::Result<Option<CUEInfo>> {
    let audio_info = audio_preprocess(flac_path).await?;
    if audio_info.embedded_cue.is_some() {
        let cue_name = PathBuf::from(audio_info.path.file_name().unwrap_or_default());
        let files_info = process_cue_helper(None, None::<PathBuf>, vec![(0, audio_info)])?;

        Ok(Some(CUEInfo {
            cue_name,
            passthrough_files: vec![],
            files_info,
        }))
    } else {
        Ok(None)
    }
}

pub async fn process_cue(cue_path: impl AsRef<Path>) -> anyhow::Result<CUEInfo> {
    let cue_file = tokio::fs::File::open(cue_path.as_ref()).await?;
    let mut cue_reader = tokio::io::BufReader::new(cue_file);
    let cue = read_cue(&mut cue_reader).await?;
    let cue = Arc::new(cue);

    let cue_file_tracks = cue
        .tracks
        .iter()
        .enumerate()
        .map(|(tid, (i, _))| (tid, i))
        .chunk_by(|(_, i)| **i)
        .into_iter()
        .map(|(i, chunks)| {
            let chunks = chunks.into_iter().map(|(tid, _)| tid).collect::<Vec<_>>();
            (i, chunks.len(), chunks)
        })
        .collect::<Vec<_>>();
    let passthrough_ids = cue_file_tracks
        .into_iter()
        .filter(|(_, count, _)| *count == 1)
        .map(|(i, _, tracks_id)| (i, tracks_id))
        .collect::<Vec<_>>();
    let passthrough_files = passthrough_ids
        .iter()
        .map(|(id, tracks_id)| {
            let mut file_path = cue_path.as_ref().to_path_buf();
            file_path.pop();
            file_path.push(&cue.files[*id]);

            let tracks_info = tracks_id
                .iter()
                .map(|track_id| TrackInfo {
                    track_id:   *track_id,
                    sample_pos: 0,
                })
                .collect();

            FileInfo {
                file_path,
                cue: cue.clone(),
                tracks_info,
                // The following flac info are only used for virtual file size estimation and thus
                // do not matter for passthrough files
                sample_rate: 0,
                total_samples: 0,
                channels: 0,
                bits_per_sample: 0,
                wav_info: None,
            }
        })
        .collect::<Vec<_>>();

    let cue_path_async = cue_path.as_ref().to_path_buf();
    let audio_infos: Vec<anyhow::Result<_>> = futures::stream::iter(
        cue.files
            .iter()
            .enumerate()
            .filter(|(id, _)| !passthrough_ids.iter().any(|(file_id, _)| id == file_id)),
    )
    .map(|(id, flac_name)| {
        let cue_path_async = cue_path_async.clone();
        async move {
            let mut path = cue_path_async;
            path.pop();
            path.push(flac_name);
            Ok((id, audio_preprocess(path).await?))
        }
    })
    .buffer_unordered(4)
    .boxed()
    .collect()
    .await;
    let audio_infos = audio_infos
        .into_iter()
        .collect::<anyhow::Result<Vec<_>>>()?;

    let files_info = process_cue_helper(Some(cue), Some(cue_path), audio_infos)?;

    Ok(CUEInfo {
        cue_name: cue_path_async,
        passthrough_files,
        files_info,
    })
}

fn process_cue_helper(
    cue: Option<Arc<CUEFile>>,
    cue_path: Option<impl AsRef<Path>>,
    audio_infos: Vec<(usize, AudioBasicInfo)>,
) -> anyhow::Result<Vec<FileInfo>> {
    audio_infos
        .into_iter()
        .map(|(file_id, audio_info)| {
            let embedded = audio_info.embedded_cue.is_some();
            let cue = match audio_info.embedded_cue {
                Some(cue) => Arc::new(cue),
                None => cue.as_ref().unwrap().clone(),
            };

            let tracks_info = parse_cue_tracks_info(&cue, &file_id, audio_info.sample_rate)
                .ok_or_else(|| anyhow!("Invalid cue file"))?;

            Ok(FileInfo {
                file_path: if embedded {
                    audio_info.path
                } else {
                    let mut file_path = cue_path.as_ref().unwrap().as_ref().to_path_buf();
                    file_path.pop();
                    file_path.push(&cue.files[file_id]);
                    file_path
                },
                cue,
                tracks_info,
                sample_rate: audio_info.sample_rate,
                total_samples: audio_info.total_samples,
                channels: audio_info.channels,
                bits_per_sample: audio_info.bits_per_sample,
                wav_info: audio_info.wav_info,
            })
        })
        .collect()
}

fn parse_cue_tracks_info(
    cue: &CUEFile,
    file_id: &cue_rw::FileID,
    sample_rate: u32,
) -> Option<Vec<TrackInfo>> {
    cue.tracks
        .iter()
        .enumerate()
        .filter(|(_, (i, _))| i == file_id)
        .map(|(tid, (_, t))| {
            let (_, ts) = t.indices.iter().find(|(id, _)| *id == 1)?;
            let sample_pos = {
                let seconds = Rational32::from(*ts);
                let seconds = Rational64::new(*seconds.numer() as i64, *seconds.denom() as i64);
                let samples = seconds * Rational64::from(sample_rate as i64);
                samples.to_integer() as u64
            };
            Some(TrackInfo {
                track_id: tid,
                sample_pos,
            })
        })
        .collect()
}

#[derive(Default)]
struct AudioBasicInfo {
    path:            PathBuf,
    embedded_cue:    Option<CUEFile>,
    sample_rate:     u32,
    total_samples:   u64,
    channels:        u8,
    bits_per_sample: u8,
    wav_info:        Option<WavInfo>,
}

async fn audio_preprocess(file_path: impl AsRef<Path>) -> anyhow::Result<AudioBasicInfo> {
    let file = tokio::fs::File::open(file_path.as_ref()).await?;
    let mut reader = tokio::io::BufReader::new(file);

    let extension = file_path.as_ref().extension();
    if extension == Some(OsStr::new("flac")) {
        let mut flac_header = [0; 4];
        reader.read_exact(&mut flac_header).await?;
        if &flac_header != b"fLaC" {
            panic!();
        }

        let metadata_blocks = get_metadata_blocks(&mut reader).await?;

        let mut embedded_cue = None;
        let vorbis_comment = metadata_blocks
            .iter()
            .find(|b| b.block_type == FlacMetadataBlockType::VorbisComment);
        if let Some(FlacMetadataBlockContent::VorbisComment(ref vorbis_comment)) =
            vorbis_comment.map(|b| &b.content)
        {
            if let Some(cue_str) = vorbis_comment.get(&String::from("cuesheet")) {
                if let Ok(cue) = CUEFile::try_from(cue_str.as_str()) {
                    embedded_cue = Some(cue);
                }
            } else if let Some(cue_str) = vorbis_comment.get(&String::from("CUESHEET")) {
                if let Ok(cue) = CUEFile::try_from(cue_str.as_str()) {
                    embedded_cue = Some(cue);
                }
            }
        }

        let stream_info =
            get_stream_info(&metadata_blocks).ok_or_else(|| anyhow!("Invalid flac file"))?;
        let sample_rate = stream_info.get_sample_rate();
        let total_samples = stream_info.sample_count;
        let channels = stream_info.get_channels();
        let bits_per_sample = stream_info.get_bits();

        Ok(AudioBasicInfo {
            path: file_path.as_ref().to_path_buf(),
            embedded_cue,
            sample_rate,
            total_samples,
            channels,
            bits_per_sample,
            wav_info: None,
        })
    } else if extension == Some(OsStr::new("wav")) {
        let wav_info = WavInfo::read_wav(&mut reader).await?;

        Ok(AudioBasicInfo {
            path:            file_path.as_ref().to_path_buf(),
            embedded_cue:    None,
            sample_rate:     wav_info.format.sample_rate,
            total_samples:   wav_info.total_samples(),
            channels:        wav_info.format.channels as _,
            bits_per_sample: wav_info.format.bits_per_sample as _,
            wav_info:        Some(wav_info),
        })
    } else {
        anyhow::bail!(
            "Invalid file for processing (not flac or wav): {}",
            file_path.as_ref().display()
        )
    }
}

fn get_stream_info(blocks: &[FlacMetadataBlock]) -> Option<&StreamInfoBlock> {
    blocks
        .iter()
        .find(|b| b.block_type == FlacMetadataBlockType::StreamInfo)
        .and_then(|b| {
            if let FlacMetadataBlockContent::StreamInfo(ref content) = b.content {
                Some(content)
            } else {
                None
            }
        })
}

fn process_metadata(
    vorbis_comment: &mut VorbisCommentBlock,
    cue: &CUEFile,
    track_id: usize,
    track_total: usize,
) {
    vorbis_comment.user_comments.remove("cuesheet");
    vorbis_comment.user_comments.remove("CUESHEET");

    let Some((_, cue_track)) = cue.tracks.get(track_id) else {
        return;
    };

    vorbis_comment.add_vorbis_comment("ALBUMARTIST", cue.performer.clone());
    vorbis_comment.add_vorbis_comment("ALBUM", cue.title.clone());
    if let Some(ref catalog) = cue.catalog {
        vorbis_comment.add_vorbis_comment("CATALOG", catalog.clone());
    }

    if let Some(date) = cue
        .comments
        .iter()
        .find(|line| line.starts_with("REM DATE"))
        .and_then(|line| line.split_once("REM DATE ").map(|(_, date)| date))
    {
        vorbis_comment.add_vorbis_comment("DATE", date.to_string());
    }

    vorbis_comment.add_vorbis_comment("TITLE", cue_track.title.clone());
    let performer = cue_track
        .performer
        .as_ref()
        .unwrap_or(&cue.performer)
        .clone();
    vorbis_comment.add_vorbis_comment("ARTIST", performer);
    if let Some(isrc) = cue_track.isrc.as_ref() {
        vorbis_comment.add_vorbis_comment("ISRC", isrc.clone());
    }
    vorbis_comment.add_vorbis_comment("TRACKNUMBER", (track_id + 1).to_string());
    vorbis_comment.add_vorbis_comment("TRACKTOTAL", track_total.to_string());
}

pub struct FlacCacheData {
    frame_sizes:             FileFrameSizes,
    frames_sample_pos:       Vec<u64>,
    tracks_head_tail_frames: Vec<EncodedTrackFrames>,
    mtime:                   i64,
}

pub async fn process_file(
    libflac_tools: &mut LibFlacDecEnc,
    writer: &mut (impl Write + Seek),
    file_path: impl AsRef<Path>,
    track_id: usize,
    cue: &CUEFile,
    tracks_info: &[TrackInfo],
    cache_data: Option<&FlacCacheData>,
) -> anyhow::Result<Option<FlacCacheData>> {
    let (decoder, encoder) = libflac_tools.split();
    let file = tokio::fs::File::open(file_path.as_ref()).await?;
    let mtime = file.metadata().await?.mtime();
    let mut reader_init = tokio::io::BufReader::new(file);

    reader_init.seek(SeekFrom::Start(4)).await?; // Skip "fLaC" header
    let metadata_blocks = get_metadata_blocks(&mut reader_init).await?;
    let stream_info =
        get_stream_info(&metadata_blocks).ok_or_else(|| anyhow!("Invalid flac file"))?;

    // libflac is blocking anyway, so we can just use blocking I/O here
    let mut reader: Box<dyn SeekableRead> = Box::new(std::io::BufReader::new(std::fs::File::open(
        file_path.as_ref(),
    )?));

    let mut frame_sizes = vec![];
    let mut frames_sample_pos = vec![];
    let mut tracks_head_tail_frames = vec![];

    let start_offset_ref;
    let frame_sizes_ref;
    let frames_sample_pos_ref;
    let tracks_head_tail_frames_ref;

    let tracks_sample_pos = tracks_info
        .iter()
        .map(|ti| ti.sample_pos)
        .collect::<Vec<_>>();

    let cache_valid = cache_data.is_some() && cache_data.unwrap().mtime >= mtime;
    if cache_valid {
        let cache_data = cache_data.unwrap();
        start_offset_ref = cache_data.frame_sizes.start_offset;
        frame_sizes_ref = &cache_data.frame_sizes.frame_sizes;
        frames_sample_pos_ref = &cache_data.frames_sample_pos;
        tracks_head_tail_frames_ref = &cache_data.tracks_head_tail_frames;
    } else {
        let ret = get_frame_sizes(decoder, reader, file_path.as_ref().display())?;

        reader = ret.reader;

        start_offset_ref = ret.data.start_offset;
        frame_sizes = ret.data.frame_sizes;
        frame_sizes_ref = &frame_sizes;

        reader.seek(SeekFrom::Start(start_offset_ref))?;
        frames_sample_pos =
            FlacFrame::scan_frames(&mut reader, frame_sizes.iter().copied(), stream_info)?;
        frames_sample_pos_ref = &frames_sample_pos;

        let ret = encode_track_head_tail_frames(
            decoder,
            encoder,
            stream_info,
            reader,
            &tracks_sample_pos,
            frames_sample_pos_ref,
            file_path.as_ref().display(),
        )?;

        reader = ret.reader;
        tracks_head_tail_frames = ret.data;
        tracks_head_tail_frames_ref = &tracks_head_tail_frames;
    }

    let mut metadata_blocks = metadata_blocks
        .iter()
        .filter(|block| {
            block.block_type != FlacMetadataBlockType::SeekTable
                && block.block_type != FlacMetadataBlockType::CUESheet
        })
        .cloned()
        .collect::<Vec<_>>();

    let tracks_info_id = tracks_info
        .iter()
        .position(|t| t.track_id == track_id)
        .ok_or_else(|| anyhow!("Invalid track id"))?;
    let track_total = cue.tracks.len();

    let track_pos = tracks_sample_pos
        .get(tracks_info_id)
        .ok_or_else(|| anyhow!("Invalid track id"))?;
    let next_track_pos = tracks_sample_pos.get(tracks_info_id + 1).copied();
    let head_tail_frames = tracks_head_tail_frames_ref
        .get(tracks_info_id)
        .ok_or_else(|| anyhow!("Invalid track id"))?;

    let FlacMetadataBlock {
        content: FlacMetadataBlockContent::VorbisComment(vorbis_comment),
        ..
    } = (match metadata_blocks
        .iter_mut()
        .find(|b| b.block_type == FlacMetadataBlockType::VorbisComment)
    {
        Some(block) => block,
        None => {
            let mut new_block = FlacMetadataBlock {
                is_last:    false,
                block_type: FlacMetadataBlockType::VorbisComment,
                content:    FlacMetadataBlockContent::VorbisComment(VorbisCommentBlock::new()),
            };

            if metadata_blocks.len() == 1 {
                // We need to ensure the stream info block is the first one
                metadata_blocks[0].is_last = false;
                new_block.is_last = true;
            }

            metadata_blocks.push(new_block);
            metadata_blocks.last_mut().unwrap()
        }
    })
    else {
        unreachable!()
    };
    process_metadata(vorbis_comment, cue, track_id, track_total);
    metadata_blocks.sort_by_key(|block| block.is_last);

    reader.seek(SeekFrom::Start(start_offset_ref))?;
    write_track(
        &mut reader,
        writer,
        &metadata_blocks,
        *track_pos,
        next_track_pos,
        &head_tail_frames.head_frames,
        head_tail_frames.tail_frames.as_ref(),
        frame_sizes_ref,
        frames_sample_pos_ref,
    )?;

    Ok(if cache_valid {
        None
    } else {
        Some(FlacCacheData {
            frame_sizes: FileFrameSizes {
                start_offset: start_offset_ref,
                frame_sizes,
            },
            frames_sample_pos,
            tracks_head_tail_frames,
            mtime,
        })
    })
}

async fn get_metadata_blocks(
    mut reader: impl AsyncRead + Unpin,
) -> Result<Vec<FlacMetadataBlock>, FlacParseError> {
    let mut metadata_blocks = vec![];
    loop {
        let metadata_block = FlacMetadataBlock::read_block(&mut reader).await?;
        let is_last = metadata_block.is_last;

        metadata_blocks.push(metadata_block);
        if is_last {
            break;
        }
    }

    Ok(metadata_blocks)
}

async fn read_cue(mut cue_reader: impl AsyncRead + Unpin) -> anyhow::Result<CUEFile> {
    let mut cue_bytes = Vec::new();
    cue_reader.read_to_end(&mut cue_bytes).await?;
    let mut cue_str = None;
    for encoding in [
        encoding_rs::UTF_8,
        encoding_rs::GBK,
        encoding_rs::GB18030,
        encoding_rs::SHIFT_JIS,
        encoding_rs::BIG5,
        encoding_rs::UTF_16LE,
    ] {
        let (str, _, failed) = encoding.decode(&cue_bytes);
        if !failed {
            cue_str = Some(str);
            break;
        }
    }

    let cue_str = cue_str.ok_or_else(|| anyhow!("Invalid cue encoding"))?;
    Ok(CUEFile::try_from(&*cue_str)?)
}

struct RetWithReader<T> {
    reader: Box<dyn SeekableRead>,
    data:   T,
}

struct FileFrameSizes {
    start_offset: u64,
    frame_sizes:  Vec<u64>,
}

/// The start byte offset of flac frames, and frame sizes
fn get_frame_sizes(
    decoder: &mut FlacDecoder,
    mut reader: impl Read + Seek + 'static,
    path_for_logging: impl ToString,
) -> std::io::Result<RetWithReader<FileFrameSizes>> {
    reader.seek(SeekFrom::Start(0))?;
    let reader = Box::new(reader);
    decoder.init(reader, path_for_logging.to_string());

    let frame_byte_offsets = decoder.scan_frames();
    let frame_sizes = frame_byte_offsets
        .windows(2)
        .map(|w| w[1] - w[0])
        .collect::<Vec<_>>();

    let reader = decoder.finish();
    Ok(RetWithReader {
        reader,
        data: FileFrameSizes {
            start_offset: frame_byte_offsets[0],
            frame_sizes,
        },
    })
}

struct EncodedTrackFrames {
    head_frames: Vec<Vec<u8>>,
    tail_frames: Option<Vec<Vec<u8>>>,
}

fn encode_track_head_tail_frames(
    decoder: &mut FlacDecoder,
    encoder: &mut FlacEncoder,
    stream_info: &StreamInfoBlock,
    mut reader: impl Read + Seek + 'static,
    tracks_sample_pos: &[u64],
    frames_sample_pos: &[u64],
    path_for_logging: impl ToString,
) -> anyhow::Result<RetWithReader<Vec<EncodedTrackFrames>>> {
    reader.seek(SeekFrom::Start(0))?;
    decoder.init(Box::new(reader), path_for_logging.to_string());

    let mut tracks_head_tail_data = vec![];
    let mut frame_data = None;
    for track_pos_chunk in tracks_sample_pos.windows(2) {
        let &[track_pos, next_track_pos] = track_pos_chunk else {
            unreachable!()
        };

        let head = frame_data.take().unwrap_or_else(|| {
            decoder.seek(track_pos);
            decoder.decode_frame().unwrap()
        });

        let tail_frame_start = *frames_sample_pos
            .iter()
            .rfind(|&&pos| pos <= next_track_pos)
            .unwrap();
        decoder.seek(tail_frame_start);

        let Some(tail_frame) = decoder.decode_frame() else {
            Err(anyhow!("error while decoding frames"))?
        };

        let (tail, next_head): (FlacFrameData, FlacFrameData) = tail_frame
            .iter()
            .map(|ch_data| ch_data.split_at((next_track_pos - tail_frame_start) as usize))
            .map(|(slice_a, slice_b)| (slice_a.to_vec(), slice_b.to_vec()))
            .unzip();
        tracks_head_tail_data.push((head, Some(tail)));

        frame_data = Some(next_head);
    }
    tracks_head_tail_data.push((frame_data.unwrap(), None));
    let reader = decoder.finish();

    let tracks_head_tail_frames = tracks_head_tail_data
        .into_iter()
        .map(|(head, tail)| {
            encoder.set_params(
                stream_info.get_channels(),
                stream_info.get_bits(),
                stream_info.get_sample_rate(),
                Some(head[0].len() as _),
            );
            encoder.init_stream();
            encoder.queue_encode(&head);
            let encoded_bytes = encoder
                .finish()
                .ok_or_else(|| anyhow!("error while encoding frames"))?;
            let head_frames = extract_frames(decoder, &encoded_bytes);

            let tail_frames = match tail {
                Some(tail) => {
                    encoder.set_params(
                        stream_info.get_channels(),
                        stream_info.get_bits(),
                        stream_info.get_sample_rate(),
                        Some(tail[0].len() as _),
                    );
                    encoder.init_stream();
                    encoder.queue_encode(&tail);
                    let encoded_bytes = encoder
                        .finish()
                        .ok_or_else(|| anyhow!("error while encoding frames"))?;
                    Some(extract_frames(decoder, &encoded_bytes))
                }
                None => None,
            };

            Ok(EncodedTrackFrames {
                head_frames,
                tail_frames,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(RetWithReader {
        reader,
        data: tracks_head_tail_frames,
    })
}

/// Unpacks frames from encoded flac bytes, omitting all metadata blocks
fn extract_frames(decoder: &mut FlacDecoder, bytes: &[u8]) -> Vec<Vec<u8>> {
    let vec = bytes.to_vec();
    let cursor = Cursor::new(vec);
    decoder.init(Box::new(cursor), String::new());
    let frame_offsets = decoder.scan_frames();
    decoder.finish();

    let mut out = vec![];
    let mut last_offset = frame_offsets[0] as usize;
    let (_header, mut left) = bytes.split_at(last_offset);
    let mut frame;

    for offset in frame_offsets.into_iter().skip(1) {
        (frame, left) = left.split_at(offset as usize - last_offset);
        last_offset = offset as usize;
        out.push(frame.to_vec());
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn write_track(
    mut reader: impl Read + Seek,
    mut writer: impl Write + Seek,
    metadata_blocks: &[FlacMetadataBlock],
    track_pos: u64,
    next_track_pos: Option<u64>,
    head_frames: &[Vec<u8>],
    tail_frames: Option<&Vec<Vec<u8>>>,
    frame_sizes: &[u64],
    frames_sample_pos: &[u64],
) -> anyhow::Result<()> {
    writer.write_all(b"fLaC")?;

    let stream_info =
        get_stream_info(metadata_blocks).ok_or_else(|| anyhow!("Invalid metadata blocks"))?;
    for block in metadata_blocks.iter() {
        block.write_block(&mut writer)?;
    }

    let head_frames = head_frames
        .iter()
        .map(|bytes| FlacFrame::read_frame(&**bytes, stream_info, bytes.len()))
        .collect::<Result<Vec<_>, _>>()?;
    let mut head_frames = Some(head_frames); // Makes borrow checker happy
    let tail_frames = match tail_frames {
        Some(frames) => Some(
            frames
                .iter()
                .map(|bytes| FlacFrame::read_frame(&**bytes, stream_info, bytes.len()))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        None => None,
    };

    let mut min_block_size = u16::MAX;
    let mut max_block_size = u16::MIN;

    let mut track_sample_pos = 0;
    let mut first_frame = true;
    let mut fixed_block_size_applied = false;

    for (size, frame_pos) in zip(frame_sizes, frames_sample_pos) {
        if *frame_pos < track_pos {
            reader.seek(SeekFrom::Current(*size as i64))?;
            continue;
        }

        let mut frame = FlacFrame::read_frame(&mut reader, stream_info, *size as _)?;
        let block_size = frame.metadata.block_size.get_size();

        if first_frame {
            // This will only be executed once
            let head_frames = head_frames.take().unwrap();
            for mut frame in head_frames {
                frame.metadata.blocking_strategy = FlacBlockingStrategy::Variable;
                frame.metadata.position = FlacFramePosition::SampleCount(track_sample_pos);
                let frame_samples = frame.metadata.block_size.get_size();
                track_sample_pos += frame_samples as u64;

                min_block_size = std::cmp::min(min_block_size, frame_samples);
                max_block_size = std::cmp::max(max_block_size, frame_samples);
                writer.write_all(&frame.into_bytes())?;
            }
            first_frame = false;
        }

        if !fixed_block_size_applied {
            min_block_size = std::cmp::min(min_block_size, block_size);
            max_block_size = std::cmp::max(max_block_size, block_size);
            if frame.metadata.blocking_strategy == flac::FlacBlockingStrategy::Fixed {
                fixed_block_size_applied = true;
            }
        }

        if next_track_pos.is_some() && *frame_pos > next_track_pos.unwrap() {
            let tail_frames = tail_frames.unwrap();
            for mut frame in tail_frames {
                frame.metadata.blocking_strategy = FlacBlockingStrategy::Variable;
                frame.metadata.position = FlacFramePosition::SampleCount(track_sample_pos);
                let frame_samples = frame.metadata.block_size.get_size();
                track_sample_pos += frame_samples as u64;

                min_block_size = std::cmp::min(min_block_size, frame_samples);
                max_block_size = std::cmp::max(max_block_size, frame_samples);
                writer.write_all(&frame.into_bytes())?;
            }
            break;
        } else {
            frame.metadata.blocking_strategy = FlacBlockingStrategy::Variable;
            frame.metadata.position = FlacFramePosition::SampleCount(track_sample_pos);
            writer.write_all(&frame.into_bytes())?;
            track_sample_pos += block_size as u64;
        }
    }

    let mut stream_info = stream_info.clone();
    stream_info.min_block_size = min_block_size;
    stream_info.max_block_size = max_block_size;
    stream_info.min_frame_size = 0;
    stream_info.max_frame_size = 0;
    stream_info.sample_count = track_sample_pos;
    stream_info.md5 = [0; 16];
    let block = FlacMetadataBlock {
        is_last:    metadata_blocks.len() == 1,
        block_type: FlacMetadataBlockType::StreamInfo,
        content:    FlacMetadataBlockContent::StreamInfo(stream_info),
    };

    writer.seek(SeekFrom::Start(4))?;
    block.write_block(writer)?;

    Ok(())
}
