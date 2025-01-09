use std::{
    ffi::{CStr, c_char, c_void},
    io::{ErrorKind, Read, Seek, SeekFrom},
};

use libflac_sys::*;

pub trait SeekableRead: Read + Seek {}
impl<T: Read + Seek> SeekableRead for T {}

pub type FlacFrameData = Vec<Vec<FLAC__int32>>;

pub struct DecoderClientData {
    reader:  Box<dyn SeekableRead>,
    path:    String,
    decoded: Option<FlacFrameData>,
}

pub struct FlacDecoder {
    inner:       *mut FLAC__StreamDecoder,
    client_data: Option<DecoderClientData>,
}

unsafe impl Send for FlacDecoder {}
unsafe impl Sync for FlacDecoder {}

impl FlacDecoder {
    pub fn new() -> Self {
        let decoder = unsafe {
            let decoder = FLAC__stream_decoder_new();
            FLAC__stream_decoder_set_metadata_ignore_all(decoder);
            FLAC__stream_decoder_set_md5_checking(decoder, false.into());
            decoder
        };

        Self {
            inner:       decoder,
            client_data: None,
        }
    }

    /// `path` is only for logging purposes
    pub fn init(&mut self, reader: Box<dyn SeekableRead>, path: String) {
        let client_data = DecoderClientData {
            reader,
            path,
            decoded: None,
        };

        self.client_data = Some(client_data);

        unsafe {
            FLAC__stream_decoder_init_stream(
                self.inner,
                Some(decoder_read_cb),
                Some(decoder_seek_cb),
                Some(decoder_tell_cb),
                Some(decoder_length_cb),
                Some(decoder_eof_cb),
                Some(decoder_write_cb),
                None,
                Some(decoder_err_cb),
                &mut self.client_data as *mut _ as *mut c_void,
            );
        }
    }

    pub fn finish(&mut self) -> Box<dyn SeekableRead> {
        unsafe {
            FLAC__stream_decoder_finish(self.inner);
        }

        self.client_data.take().unwrap().reader
    }

    pub fn cleanup(&mut self) {
        unsafe {
            FLAC__stream_decoder_finish(self.inner);
        }
    }

    pub fn scan_frames(&mut self) -> Vec<u64> {
        let mut frame_start_indices = vec![];

        unsafe {
            FLAC__stream_decoder_process_until_end_of_metadata(self.inner);

            let mut position = 0;
            FLAC__stream_decoder_get_decode_position(self.inner, &mut position);
            frame_start_indices.push(position);

            loop {
                FLAC__stream_decoder_skip_single_frame(self.inner);
                if FLAC__stream_decoder_get_state(self.inner) == FLAC__STREAM_DECODER_END_OF_STREAM
                {
                    break;
                }
                FLAC__stream_decoder_get_decode_position(self.inner, &mut position);
                frame_start_indices.push(position);
            }
        }

        frame_start_indices
    }

    pub fn seek(&mut self, sample_position: u64) -> bool {
        unsafe { FLAC__stream_decoder_seek_absolute(self.inner, sample_position) != 0 }
    }

    pub fn decode_frame(&mut self) -> Option<FlacFrameData> {
        unsafe {
            let success = FLAC__stream_decoder_process_single(self.inner);
            if success != 0 {
                self.client_data.as_mut()?.decoded.take()
            } else {
                None
            }
        }
    }
}

unsafe extern "C" fn decoder_read_cb(
    _decoder: *const FLAC__StreamDecoder,
    buffer: *mut FLAC__byte,
    bytes: *mut usize,
    client_data: *mut c_void,
) -> FLAC__StreamDecoderReadStatus {
    let client_data = client_data as *mut DecoderClientData;

    unsafe {
        let buffer = std::slice::from_raw_parts_mut(buffer, *bytes);
        match (*client_data).reader.read(buffer) {
            Ok(bytes_read) => {
                *bytes = bytes_read;
                if !buffer.is_empty() && bytes_read == 0 {
                    FLAC__STREAM_DECODER_READ_STATUS_END_OF_STREAM
                } else {
                    FLAC__STREAM_DECODER_READ_STATUS_CONTINUE
                }
            }
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                *bytes = 0;
                FLAC__STREAM_DECODER_READ_STATUS_END_OF_STREAM
            }
            Err(e) => {
                let path = (*client_data).path.as_str();
                tracing::error!("Error while reading FLAC file {path}: {e}");

                *bytes = 0;
                FLAC__STREAM_DECODER_READ_STATUS_ABORT
            }
        }
    }
}

unsafe extern "C" fn decoder_seek_cb(
    _decoder: *const FLAC__StreamDecoder,
    absolute_byte_offset: FLAC__uint64,
    client_data: *mut c_void,
) -> FLAC__StreamDecoderSeekStatus {
    let client_data = client_data as *mut DecoderClientData;

    unsafe {
        match (*client_data)
            .reader
            .seek(SeekFrom::Start(absolute_byte_offset))
        {
            Ok(_) => FLAC__STREAM_DECODER_SEEK_STATUS_OK,
            Err(e) => {
                let path = (*client_data).path.as_str();
                tracing::error!("Error while reading FLAC file {path}: {e}");

                FLAC__STREAM_DECODER_SEEK_STATUS_ERROR
            }
        }
    }
}

unsafe extern "C" fn decoder_tell_cb(
    _decoder: *const FLAC__StreamDecoder,
    absolute_byte_offset: *mut FLAC__uint64,
    client_data: *mut c_void,
) -> FLAC__StreamDecoderTellStatus {
    let client_data = client_data as *mut DecoderClientData;

    unsafe {
        match (*client_data).reader.stream_position() {
            Ok(pos) => {
                *absolute_byte_offset = pos;
                FLAC__STREAM_DECODER_TELL_STATUS_OK
            }
            Err(e) => {
                let path = (*client_data).path.as_str();
                tracing::error!("Error while reading FLAC file {path}: {e}");

                FLAC__STREAM_DECODER_TELL_STATUS_ERROR
            }
        }
    }
}

unsafe extern "C" fn decoder_length_cb(
    _decoder: *const FLAC__StreamDecoder,
    stream_length: *mut FLAC__uint64,
    client_data: *mut c_void,
) -> FLAC__StreamDecoderLengthStatus {
    let client_data = client_data as *mut DecoderClientData;

    unsafe {
        match (*client_data).reader.stream_len() {
            Ok(len) => {
                *stream_length = len;
                FLAC__STREAM_DECODER_LENGTH_STATUS_OK
            }
            Err(e) => {
                let path = (*client_data).path.as_str();
                tracing::error!("Error while reading FLAC file {path}: {e}");

                FLAC__STREAM_DECODER_LENGTH_STATUS_ERROR
            }
        }
    }
}

unsafe extern "C" fn decoder_eof_cb(
    _decoder: *const FLAC__StreamDecoder,
    _client_data: *mut c_void,
) -> FLAC__bool {
    // We cannot know whether eof reached for objects without `BufRead` trait
    false.into()
}

unsafe extern "C" fn decoder_write_cb(
    _decoder: *const FLAC__StreamDecoder,
    frame: *const FLAC__Frame,
    buffer: *const *const FLAC__int32,
    client_data: *mut c_void,
) -> FLAC__StreamDecoderWriteStatus {
    unsafe {
        let channels = (*frame).header.channels;
        let sample_size = (*frame).header.blocksize;

        let buffers = std::slice::from_raw_parts(buffer, channels as usize);
        let buffers = buffers
            .iter()
            .map(|&buf| std::slice::from_raw_parts(buf, sample_size as usize).to_vec())
            .collect();

        let client_data = client_data as *mut DecoderClientData;
        (*client_data).decoded = Some(buffers);
    }

    FLAC__STREAM_DECODER_WRITE_STATUS_CONTINUE
}

unsafe extern "C" fn decoder_err_cb(
    decoder: *const FLAC__StreamDecoder,
    status: FLAC__StreamDecoderErrorStatus,
    client_data: *mut c_void,
) {
    let client_data = client_data as *mut DecoderClientData;
    let path = unsafe { (*client_data).path.as_str() };

    let error_str = unsafe {
        let error_status_strings = &FLAC__StreamDecoderErrorStatusString as *const *const c_char;
        let error_str = error_status_strings.add(status as _);
        CStr::from_ptr(*error_str).to_str().unwrap()
    };

    let mut byte_offset = 0;
    FLAC__stream_decoder_get_decode_position(decoder, &mut byte_offset);
    tracing::error!("Error while decoding FLAC file: {path}: {error_str} at 0x{byte_offset:X}");
}

impl Drop for FlacDecoder {
    fn drop(&mut self) {
        unsafe { FLAC__stream_decoder_delete(self.inner) }
    }
}

pub struct EncoderClientData {
    buffer: Vec<u8>,
}

pub struct FlacEncoder {
    inner:       *mut FLAC__StreamEncoder,
    client_data: EncoderClientData,
}

unsafe impl Send for FlacEncoder {}
unsafe impl Sync for FlacEncoder {}

impl FlacEncoder {
    pub fn new() -> Self {
        Self {
            inner:       unsafe { FLAC__stream_encoder_new() },
            client_data: EncoderClientData { buffer: vec![] },
        }
    }

    pub fn set_params(
        &mut self,
        channels: u8,
        bits_per_sample: u8,
        sample_rate: u32,
        total_samples: Option<u64>,
    ) {
        unsafe {
            FLAC__stream_encoder_set_channels(self.inner, channels as u32);
            FLAC__stream_encoder_set_bits_per_sample(self.inner, bits_per_sample as u32);
            FLAC__stream_encoder_set_sample_rate(self.inner, sample_rate);

            if let Some(total_samples) = total_samples {
                FLAC__stream_encoder_set_total_samples_estimate(self.inner, total_samples);
            }
        }
    }

    pub fn init_stream(&mut self) {
        unsafe {
            FLAC__stream_encoder_init_stream(
                self.inner,
                Some(encoder_write_cb),
                None,
                None,
                None,
                &mut self.client_data as *mut _ as *mut c_void,
            );
        }
    }

    pub fn queue_encode(&mut self, data: &FlacFrameData) -> bool {
        unsafe {
            let samples = data[0].len();
            let data = data
                .iter()
                .map(|channel_data| channel_data.as_ptr())
                .collect::<Vec<_>>();

            // This function will copy data from buffer, so `data` is valid.
            FLAC__stream_encoder_process(self.inner, data.as_ptr(), samples as _) != 0
        }
    }

    pub fn finish(&mut self) -> Option<Vec<u8>> {
        let ret = if unsafe { FLAC__stream_encoder_finish(self.inner) != 0 } {
            let data = self.client_data.buffer.clone();
            Some(data)
        } else {
            None
        };
        self.client_data.buffer.clear();

        ret
    }
}

unsafe extern "C" fn encoder_write_cb(
    _encoder: *const FLAC__StreamEncoder,
    buffer: *const FLAC__byte,
    bytes: usize,
    _samples: u32,
    _current_frame: u32,
    client_data: *mut c_void,
) -> FLAC__StreamEncoderWriteStatus {
    let buffer = std::slice::from_raw_parts(buffer, bytes);
    let client_data = client_data as *mut EncoderClientData;
    (*client_data).buffer.extend(buffer);

    FLAC__STREAM_DECODER_WRITE_STATUS_CONTINUE
}

impl Drop for FlacEncoder {
    fn drop(&mut self) {
        unsafe { FLAC__stream_encoder_delete(self.inner) }
    }
}
