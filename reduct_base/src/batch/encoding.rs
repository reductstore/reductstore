// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

//! Per-record payload encoding for batched transfers.
//!
//! A compressed record in a batch is marked with an additional header
//! `x-reduct-encoding-<ts>: <codec>,<original_length>` while the
//! `x-reduct-time-<ts>` header carries the on-wire (compressed) content length,
//! so the batch framing math stays unchanged.

use crate::error::ReductError;
use crate::unprocessable_entity;
use http::HeaderMap;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

pub const ENCODING_HEADER_PREFIX: &str = "x-reduct-encoding-";

/// Codec used to compress a record payload on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordEncoding {
    Zstd,
    Gzip,
}

impl Display for RecordEncoding {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RecordEncoding::Zstd => write!(f, "zstd"),
            RecordEncoding::Gzip => write!(f, "gzip"),
        }
    }
}

impl FromStr for RecordEncoding {
    type Err = ReductError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            "zstd" => Ok(RecordEncoding::Zstd),
            "gzip" => Ok(RecordEncoding::Gzip),
            other => Err(unprocessable_entity!("Unknown record encoding '{}'", other)),
        }
    }
}

/// Build the value for an `x-reduct-encoding-<ts>` header.
pub fn make_encoding_header_value(encoding: RecordEncoding, original_length: u64) -> String {
    format!("{},{}", encoding, original_length)
}

/// Parse an `x-reduct-encoding-<ts>` header value into a codec and the
/// decompressed (original) content length.
pub fn parse_encoding_header(value: &str) -> Result<(RecordEncoding, u64), ReductError> {
    let (codec, original_length) = value
        .split_once(',')
        .ok_or(unprocessable_entity!("Invalid encoding header"))?;
    let codec = RecordEncoding::from_str(codec)?;
    let original_length = original_length
        .trim()
        .parse::<u64>()
        .map_err(|_| unprocessable_entity!("Invalid original length in encoding header"))?;
    Ok((codec, original_length))
}

/// Collect all `x-reduct-encoding-<ts>` headers from a header map keyed by timestamp.
pub fn parse_encoding_headers(
    headers: &HeaderMap,
) -> Result<BTreeMap<u64, (RecordEncoding, u64)>, ReductError> {
    let mut encodings = BTreeMap::new();
    for (name, value) in headers.iter() {
        let name = name.as_str();
        if !name.starts_with(ENCODING_HEADER_PREFIX) {
            continue;
        }

        let time = name[ENCODING_HEADER_PREFIX.len()..]
            .parse::<u64>()
            .map_err(|_| {
                unprocessable_entity!(
                    "Invalid header '{}': must be an unix timestamp in microseconds",
                    name
                )
            })?;
        let value = value
            .to_str()
            .map_err(|err| unprocessable_entity!("Malformed header received: {}", err))?;
        encodings.insert(time, parse_encoding_header(value)?);
    }
    Ok(encodings)
}

/// Check if a MIME type is worth compressing. Returns `false` for formats that
/// are already compressed, so replication can skip them.
pub fn is_compressible_mime(content_type: &str) -> bool {
    let mime = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();

    if mime == "image/svg+xml" {
        return true;
    }

    const SKIP_PREFIXES: [&str; 3] = ["image/", "video/", "audio/"];
    if SKIP_PREFIXES.iter().any(|p| mime.starts_with(p)) {
        return false;
    }

    const SKIP_TYPES: [&str; 10] = [
        "application/zip",
        "application/gzip",
        "application/x-gzip",
        "application/zstd",
        "application/x-7z-compressed",
        "application/x-rar-compressed",
        "application/vnd.rar",
        "application/x-bzip2",
        "application/x-xz",
        "application/pdf",
    ];
    if SKIP_TYPES.contains(&mime.as_str()) || mime.ends_with("+zip") {
        return false;
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;
    use rstest::*;

    #[rstest]
    #[case(RecordEncoding::Zstd, "zstd")]
    #[case(RecordEncoding::Gzip, "gzip")]
    fn test_encoding_round_trip(#[case] encoding: RecordEncoding, #[case] name: &str) {
        assert_eq!(encoding.to_string(), name);
        assert_eq!(RecordEncoding::from_str(name).unwrap(), encoding);
    }

    #[rstest]
    fn test_unknown_encoding() {
        let err = RecordEncoding::from_str("br").err().unwrap();
        assert_eq!(err, unprocessable_entity!("Unknown record encoding 'br'"));
    }

    #[rstest]
    fn test_encoding_header_round_trip() {
        let value = make_encoding_header_value(RecordEncoding::Zstd, 1024);
        assert_eq!(value, "zstd,1024");
        assert_eq!(
            parse_encoding_header(&value).unwrap(),
            (RecordEncoding::Zstd, 1024)
        );
    }

    #[rstest]
    #[case("zstd")]
    #[case("zstd,xxx")]
    #[case("br,100")]
    #[case("")]
    fn test_parse_encoding_header_bad(#[case] value: &str) {
        assert!(parse_encoding_header(value).is_err());
    }

    #[rstest]
    fn test_parse_encoding_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("x-reduct-time-1", HeaderValue::from_static("10,text/plain"));
        headers.insert("x-reduct-encoding-1", HeaderValue::from_static("gzip,100"));
        headers.insert("x-reduct-encoding-2", HeaderValue::from_static("zstd,200"));

        let encodings = parse_encoding_headers(&headers).unwrap();
        assert_eq!(encodings.len(), 2);
        assert_eq!(encodings[&1], (RecordEncoding::Gzip, 100));
        assert_eq!(encodings[&2], (RecordEncoding::Zstd, 200));
    }

    #[rstest]
    fn test_parse_encoding_headers_bad_timestamp() {
        let mut headers = HeaderMap::new();
        headers.insert("x-reduct-encoding-yyy", HeaderValue::from_static("zstd,10"));
        assert!(parse_encoding_headers(&headers).is_err());
    }

    #[rstest]
    #[case("text/plain", true)]
    #[case("application/json", true)]
    #[case("application/octet-stream", true)]
    #[case("image/svg+xml", true)]
    #[case("text/csv; charset=utf-8", true)]
    #[case("image/jpeg", false)]
    #[case("image/jpg", false)]
    #[case("Video/MP4", false)]
    #[case("audio/mpeg", false)]
    #[case("application/zip", false)]
    #[case("application/gzip", false)]
    #[case("application/zstd", false)]
    #[case("application/pdf", false)]
    #[case("application/epub+zip", false)]
    fn test_is_compressible_mime(#[case] mime: &str, #[case] expected: bool) {
        assert_eq!(is_compressible_mime(mime), expected);
    }
}
