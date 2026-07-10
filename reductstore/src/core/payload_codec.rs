// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

//! Whole-buffer compression codecs for replicated record payloads.

use bytes::Bytes;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use reduct_base::batch::RecordEncoding;
use reduct_base::error::ReductError;
use reduct_base::{internal_server_error, unprocessable_entity};
use std::io::{Read, Write};

const ZSTD_COMPRESSION_LEVEL: i32 = 3;

/// Compress a record payload with the given codec.
pub fn compress_payload(codec: RecordEncoding, data: &[u8]) -> Result<Vec<u8>, ReductError> {
    match codec {
        RecordEncoding::Zstd => zstd::bulk::compress(data, ZSTD_COMPRESSION_LEVEL)
            .map_err(|err| internal_server_error!("Failed to compress payload: {}", err)),
        RecordEncoding::Gzip => {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder
                .write_all(data)
                .and_then(|_| encoder.finish())
                .map_err(|err| internal_server_error!("Failed to compress payload: {}", err))
        }
    }
}

/// Decompress a record payload, expecting exactly `expected_len` bytes.
///
/// The output is capped at `expected_len` to protect against decompression bombs;
/// a size mismatch in either direction is an error.
pub fn decompress_payload(
    codec: RecordEncoding,
    data: &[u8],
    expected_len: u64,
) -> Result<Bytes, ReductError> {
    let expected_len = usize::try_from(expected_len)
        .map_err(|_| unprocessable_entity!("Invalid original length in encoding header"))?;

    let mut decompressed = vec![0u8; expected_len];
    let read_exact = |reader: &mut dyn Read, buf: &mut [u8]| -> Result<(), ReductError> {
        reader
            .read_exact(buf)
            .map_err(|_| unprocessable_entity!("Failed to decompress payload"))?;
        // must be fully consumed: any extra byte means the original length is wrong
        let mut probe = [0u8; 1];
        match reader.read(&mut probe) {
            Ok(0) => Ok(()),
            _ => Err(unprocessable_entity!(
                "Decompressed payload does not match the original length"
            )),
        }
    };

    match codec {
        RecordEncoding::Zstd => {
            let mut decoder = zstd::stream::read::Decoder::new(data)
                .map_err(|_| unprocessable_entity!("Failed to decompress payload"))?;
            read_exact(&mut decoder, &mut decompressed)?;
        }
        RecordEncoding::Gzip => {
            let mut decoder = GzDecoder::new(data);
            read_exact(&mut decoder, &mut decompressed)?;
        }
    }

    Ok(Bytes::from(decompressed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    #[rstest]
    #[case(RecordEncoding::Zstd)]
    #[case(RecordEncoding::Gzip)]
    fn test_round_trip(#[case] codec: RecordEncoding) {
        let data = b"hello world, hello world, hello world".repeat(100);
        let compressed = compress_payload(codec, &data).unwrap();
        assert!(compressed.len() < data.len());

        let decompressed = decompress_payload(codec, &compressed, data.len() as u64).unwrap();
        assert_eq!(decompressed, Bytes::from(data));
    }

    #[rstest]
    #[case(RecordEncoding::Zstd)]
    #[case(RecordEncoding::Gzip)]
    fn test_round_trip_empty(#[case] codec: RecordEncoding) {
        let compressed = compress_payload(codec, b"").unwrap();
        let decompressed = decompress_payload(codec, &compressed, 0).unwrap();
        assert!(decompressed.is_empty());
    }

    #[rstest]
    #[case(RecordEncoding::Zstd)]
    #[case(RecordEncoding::Gzip)]
    fn test_corrupted_payload(#[case] codec: RecordEncoding) {
        let err = decompress_payload(codec, b"garbage", 10).err().unwrap();
        assert_eq!(err, unprocessable_entity!("Failed to decompress payload"));
    }

    #[rstest]
    #[case(RecordEncoding::Zstd, 5)]
    #[case(RecordEncoding::Gzip, 5)]
    #[case(RecordEncoding::Zstd, 1000)]
    #[case(RecordEncoding::Gzip, 1000)]
    fn test_wrong_original_length(#[case] codec: RecordEncoding, #[case] expected_len: u64) {
        let compressed = compress_payload(codec, &b"x".repeat(100)).unwrap();
        assert!(decompress_payload(codec, &compressed, expected_len).is_err());
    }
}
