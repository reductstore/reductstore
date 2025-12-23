// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Batched protocol v2
// -------------------
// Metadata:
//   x-reduct-entries: comma-separated, percent-encoded entry names (index == position)
//   x-reduct-start-ts: first timestamp in the batch
//
// Per record:
//   x-reduct-<ENTRY-INDEX>-<TIME-DELTA-uS>: batched header value
//   (error responses use the same suffix: x-reduct-error-<ENTRY-INDEX>-<TIME-DELTA-uS>)
//
// Header value rules (optimised to avoid repetition):
//   - Always includes content-length.
//   - Metadata can be omitted or sent as deltas per entry:
//       * "123" -> reuse previous content-type and labels for the entry.
//       * "123,<ct>" -> reuse previous labels; set content-type to <ct>.
//       * "123,,<label-delta>" -> reuse previous content-type; apply label delta.
//       * "123,<ct>,<label-delta>" -> explicit content-type and label delta.
//   - Label delta sends only changed labels; unset a label with `k=`. Unmentioned labels
//     keep their previous value for the entry.
//   - The first record for an entry must provide content-type and labels when re-use is
//     requested; otherwise defaults apply (content-type defaults to octet-stream, labels to empty).

use crate::batch::v1::RecordHeader;
use crate::error::ReductError;
use crate::unprocessable_entity;
use crate::Labels;
use http::{HeaderMap, HeaderName, HeaderValue};
use std::collections::HashMap;
use std::str::FromStr;

const HEADER_PREFIX: &str = "x-reduct-";
const ENTRIES_HEADER: &str = "x-reduct-entries";
const START_TS_HEADER: &str = "x-reduct-start-ts";

/// Represents a parsed batched header that includes the entry name and timestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryRecordHeader {
    pub entry: String,
    pub timestamp: u64,
    pub header: RecordHeader,
}

fn is_tchar(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

/// Percent-encode an entry name so it can be used in header values.
pub fn encode_entry_name(entry: &str) -> String {
    let mut encoded = String::with_capacity(entry.len());
    for byte in entry.as_bytes() {
        if is_tchar(*byte) {
            encoded.push(*byte as char);
        } else {
            encoded.push_str(&format!("%{:02X}", byte));
        }
    }
    encoded
}

/// Decode an entry name that was percent-encoded for use in headers.
pub fn decode_entry_name(encoded: &str) -> Result<String, ReductError> {
    let mut decoded = Vec::with_capacity(encoded.len());
    let bytes = encoded.as_bytes();
    let mut pos = 0;
    while pos < bytes.len() {
        match bytes[pos] {
            b'%' => {
                if pos + 2 >= bytes.len() {
                    return Err(unprocessable_entity!(
                        "Invalid entry encoding in header name: '{}'",
                        encoded
                    ));
                }
                let high = (bytes[pos + 1] as char).to_digit(16);
                let low = (bytes[pos + 2] as char).to_digit(16);
                if high.is_none() || low.is_none() {
                    return Err(unprocessable_entity!(
                        "Invalid entry encoding in header name: '{}'",
                        encoded
                    ));
                }
                decoded.push((high.unwrap() * 16 + low.unwrap()) as u8);
                pos += 3;
            }
            other => {
                decoded.push(other);
                pos += 1;
            }
        }
    }

    String::from_utf8(decoded)
        .map_err(|_| unprocessable_entity!("Entry name is not valid UTF-8 in header '{}'", encoded))
}

/// Parse the `x-reduct-entries` header containing a comma separated list of percent-encoded entries.
pub fn parse_entries_header(entries: &HeaderValue) -> Result<Vec<String>, ReductError> {
    let entries = entries
        .to_str()
        .map_err(|_| unprocessable_entity!("Invalid entries header"))?;
    if entries.trim().is_empty() {
        return Err(unprocessable_entity!("x-reduct-entries header is required"));
    }

    entries
        .split(',')
        .map(|entry| decode_entry_name(entry.trim()))
        .collect()
}

/// Create a header name for batched protocol v2: `x-reduct-<ENTRY-INDEX>-<TIME-DELTA>`.
pub fn make_batched_header_name(entry_index: usize, time_delta: u64) -> HeaderName {
    HeaderName::from_str(&format!("{}{}-{}", HEADER_PREFIX, entry_index, time_delta))
        .expect("Entry index and time delta must produce a valid header name")
}

/// Parse a v2 batched header name (`x-reduct-<ENTRY-INDEX>-<TIME-DELTA>`) into index and delta.
pub fn parse_batched_header_name(name: &str) -> Result<(usize, u64), ReductError> {
    if !name.starts_with(HEADER_PREFIX) {
        return Err(unprocessable_entity!("Invalid batched header '{}'", name));
    }

    let without_prefix = &name[HEADER_PREFIX.len()..];
    let (entry_index, delta) = without_prefix
        .rsplit_once('-')
        .ok_or(unprocessable_entity!("Invalid batched header '{}'", name))?;

    let entry_index: usize = entry_index.parse().map_err(|_| {
        unprocessable_entity!("Invalid header '{}': entry index must be a number", name)
    })?;

    let delta = delta.parse::<u64>().map_err(|_| {
        unprocessable_entity!(
            "Invalid header '{}': must be an unix timestamp in microseconds",
            name
        )
    })?;

    Ok((entry_index, delta))
}

fn parse_start_timestamp(headers: &HeaderMap) -> Result<u64, ReductError> {
    headers
        .get(START_TS_HEADER)
        .ok_or(unprocessable_entity!(
            "x-reduct-start-ts header is required"
        ))?
        .to_str()
        .map_err(|_| unprocessable_entity!("Invalid x-reduct-start-ts header"))?
        .parse::<u64>()
        .map_err(|_| unprocessable_entity!("Invalid x-reduct-start-ts header"))
}

fn parse_entries(headers: &HeaderMap) -> Result<Vec<String>, ReductError> {
    headers
        .get(ENTRIES_HEADER)
        .ok_or(unprocessable_entity!("x-reduct-entries header is required"))
        .and_then(parse_entries_header)
}

fn apply_label_delta(raw_labels: &str, base: &Labels) -> Result<Labels, ReductError> {
    let mut labels = base.clone();
    let mut rest = raw_labels.trim().to_string();

    if rest.is_empty() {
        return Ok(labels);
    }

    while let Some(pair) = rest.split_once('=') {
        let (key, value) = pair;
        let key = key.trim();
        if key.starts_with('@') {
            return Err(unprocessable_entity!(
                "Label names must not start with '@': reserved for computed labels",
            ));
        }

        let (value, next_rest) = if value.starts_with('\"') {
            let value = value[1..].to_string();
            let (value, rest) = value
                .split_once('\"')
                .ok_or(unprocessable_entity!("Invalid batched header"))?;
            (
                value.trim().to_string(),
                rest.trim_start_matches(',').trim().to_string(),
            )
        } else if let Some((value, rest)) = value.split_once(',') {
            (value.trim().to_string(), rest.trim().to_string())
        } else {
            (value.trim().to_string(), String::new())
        };

        if value.is_empty() {
            labels.remove(key);
        } else {
            labels.insert(key.to_string(), value);
        }

        rest = next_rest;
        if rest.is_empty() {
            break;
        }
    }

    Ok(labels)
}

fn parse_record_header_with_defaults(
    raw: &str,
    previous: Option<&RecordHeader>,
) -> Result<RecordHeader, ReductError> {
    let (content_length_str, rest_opt) = raw
        .split_once(',')
        .map(|(len, rest)| (len.trim(), Some(rest)))
        .unwrap_or((raw.trim(), None));

    let content_length = content_length_str
        .parse::<u64>()
        .map_err(|_| unprocessable_entity!("Invalid batched header"))?;

    if rest_opt.is_none() {
        let prev = previous.ok_or_else(|| {
            unprocessable_entity!(
                "Content-type and labels must be provided for the first record of an entry"
            )
        })?;
        return Ok(RecordHeader {
            content_length,
            content_type: prev.content_type.clone(),
            labels: prev.labels.clone(),
        });
    }

    let rest = rest_opt.unwrap();
    let (content_type_raw, labels_raw) = match rest.split_once(',') {
        Some((ct, labels)) => (ct, Some(labels)),
        None => (rest, None),
    };

    let content_type = if !content_type_raw.trim().is_empty() {
        content_type_raw.trim().to_string()
    } else if let Some(prev) = previous {
        prev.content_type.clone()
    } else {
        "application/octet-stream".to_string()
    };

    let labels = match labels_raw {
        None => previous
            .map(|prev| prev.labels.clone())
            .unwrap_or_else(HashMap::new),
        Some(raw_labels) => apply_label_delta(
            raw_labels,
            previous.map(|prev| &prev.labels).unwrap_or(&HashMap::new()),
        )?,
    };

    Ok(RecordHeader {
        content_length,
        content_type,
        labels,
    })
}

/// Sort and parse v2 batched headers in a header map.
///
/// Only headers following the `x-reduct-<ENTRY-INDEX>-<TIME-DELTA>` pattern are considered.
/// Entries are sorted lexicographically by entry name, then by timestamp (start + delta).
pub fn sort_headers_by_entry_and_time(
    headers: &HeaderMap,
) -> Result<Vec<(usize, u64, HeaderValue)>, ReductError> {
    let mut parsed_headers: Vec<(usize, u64, HeaderValue)> = headers
        .clone()
        .into_iter()
        .filter(|(name, _)| name.is_some())
        .map(|(name, value)| (name.unwrap().to_string(), value))
        .filter(|(name, _)| name.starts_with(HEADER_PREFIX))
        .filter(|(name, _)| name.rsplit_once('-').is_some())
        .filter(|(name, _)| {
            name.rsplit_once('-')
                .map(|(_, ts)| ts.chars().all(|ch| ch.is_ascii_digit()))
                .unwrap_or(false)
        })
        .map(|(name, value)| {
            let (entry_index, time_delta) = parse_batched_header_name(&name)?;
            Ok((entry_index, time_delta, value))
        })
        .collect::<Result<_, ReductError>>()?;

    parsed_headers.sort_by(|(idx_a, time_a, _), (idx_b, time_b, _)| {
        let ordering = idx_a.cmp(idx_b);
        if ordering == std::cmp::Ordering::Equal {
            time_a.cmp(time_b)
        } else {
            ordering
        }
    });

    Ok(parsed_headers)
}

/// Parse and sort v2 batched headers including record metadata.
pub fn parse_batched_headers(headers: &HeaderMap) -> Result<Vec<EntryRecordHeader>, ReductError> {
    let entries = parse_entries(headers)?;
    let start_ts = parse_start_timestamp(headers)?;
    let mut last_header_per_entry: HashMap<String, RecordHeader> = HashMap::new();
    let mut result = Vec::new();

    for (entry_index, delta, value) in sort_headers_by_entry_and_time(headers)? {
        let entry = entries.get(entry_index).ok_or_else(|| {
            unprocessable_entity!(
                "Invalid header '{}{}-{}': entry index out of range",
                HEADER_PREFIX,
                entry_index,
                delta
            )
        })?;

        let raw_value = value
            .to_str()
            .map_err(|_| unprocessable_entity!("Invalid batched header"))?;

        let header =
            parse_record_header_with_defaults(raw_value, last_header_per_entry.get(entry))?;
        let timestamp = start_ts + delta;

        last_header_per_entry.insert(entry.clone(), header.clone());

        result.push(EntryRecordHeader {
            entry: entry.clone(),
            timestamp,
            header,
        });
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    #[test]
    fn test_encode_entry_name_slash() {
        assert_eq!(encode_entry_name("ro/topic/1"), "ro%2Ftopic%2F1");
    }

    #[test]
    fn test_encode_entry_name_safe_chars() {
        assert_eq!(encode_entry_name("entry-1_foo~bar"), "entry-1_foo~bar");
    }

    #[test]
    fn test_decode_entry_name_roundtrip() {
        let entry = "mqtt/topic/1";
        let encoded = encode_entry_name(entry);
        assert_eq!(decode_entry_name(&encoded).unwrap(), entry);
    }

    #[test]
    fn test_decode_entry_name_invalid_percent() {
        let err = decode_entry_name("foo%ZZ").err().unwrap();
        assert_eq!(
            err,
            unprocessable_entity!("Invalid entry encoding in header name: 'foo%ZZ'")
        );
    }

    #[test]
    fn test_parse_entries_header_roundtrip() {
        let value = HeaderValue::from_str("sensor,ro%2Ftopic").unwrap();
        let entries = parse_entries_header(&value).unwrap();
        assert_eq!(entries, vec!["sensor".to_string(), "ro/topic".to_string()]);
    }

    #[test]
    fn test_parse_batched_header_name_basic() {
        let (entry_index, delta) = parse_batched_header_name("x-reduct-1-123").unwrap();
        assert_eq!(entry_index, 1);
        assert_eq!(delta, 123);
    }

    #[test]
    fn test_parse_batched_header_name_invalid_time() {
        let err = parse_batched_header_name("x-reduct-1-abc").err().unwrap();
        assert_eq!(
            err,
            unprocessable_entity!(
                "Invalid header '{}': must be an unix timestamp in microseconds",
                "x-reduct-1-abc"
            )
        );
    }

    #[test]
    fn test_sort_headers_by_entry_and_time() {
        let mut headers = HeaderMap::new();
        headers.insert(
            ENTRIES_HEADER,
            HeaderValue::from_static("sensor,ro%2Ftopic"),
        );
        headers.insert(START_TS_HEADER, HeaderValue::from_static("10"));
        headers.insert(
            make_batched_header_name(1, 5),
            HeaderValue::from_static("1,text/plain"),
        );
        headers.insert(
            make_batched_header_name(0, 2),
            HeaderValue::from_static("1,text/plain"),
        );
        headers.insert(
            make_batched_header_name(0, 3),
            HeaderValue::from_static("1,text/plain"),
        );

        let parsed = sort_headers_by_entry_and_time(&headers).unwrap();
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0].0, 0);
        assert_eq!(parsed[0].1, 2);
        assert_eq!(parsed[1].0, 0);
        assert_eq!(parsed[1].1, 3);
        assert_eq!(parsed[2].0, 1);
        assert_eq!(parsed[2].1, 5);
    }

    #[test]
    fn test_parse_batched_headers_with_values() {
        let mut headers = HeaderMap::new();
        headers.insert(ENTRIES_HEADER, HeaderValue::from_static("entry,ro%2Ftopic"));
        headers.insert(START_TS_HEADER, HeaderValue::from_static("1000"));
        headers.insert(
            make_batched_header_name(1, 15),
            HeaderValue::from_static("3,text/plain"),
        );
        headers.insert(
            make_batched_header_name(0, 10),
            HeaderValue::from_static("5,text/csv,label=value"),
        );

        let parsed = parse_batched_headers(&headers).unwrap();
        assert_eq!(parsed.len(), 2);

        assert_eq!(parsed[0].entry, "entry");
        assert_eq!(parsed[0].timestamp, 1010);
        assert_eq!(parsed[0].header.content_length, 5);
        assert_eq!(parsed[0].header.content_type, "text/csv");
        assert_eq!(parsed[0].header.labels.get("label").unwrap(), "value");

        assert_eq!(parsed[1].entry, "ro/topic");
        assert_eq!(parsed[1].timestamp, 1015);
        assert_eq!(parsed[1].header.content_length, 3);
        assert_eq!(parsed[1].header.content_type, "text/plain");
    }

    #[test]
    fn test_parse_batched_headers_reuse_metadata() {
        let mut headers = HeaderMap::new();
        headers.insert(ENTRIES_HEADER, HeaderValue::from_static("entry"));
        headers.insert(START_TS_HEADER, HeaderValue::from_static("0"));
        headers.insert(
            make_batched_header_name(0, 0),
            HeaderValue::from_static("10,text/plain,x=y"),
        );
        headers.insert(
            make_batched_header_name(0, 5),
            HeaderValue::from_static("2"),
        ); // reuse type + labels
        headers.insert(
            make_batched_header_name(0, 10),
            HeaderValue::from_static("3,,x=z"),
        ); // reuse type, override labels

        let parsed = parse_batched_headers(&headers).unwrap();
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0].header.content_type, "text/plain");
        assert_eq!(parsed[0].header.labels.get("x").unwrap(), "y");

        assert_eq!(parsed[1].header.content_type, "text/plain");
        assert_eq!(parsed[1].header.labels.get("x").unwrap(), "y");

        assert_eq!(parsed[2].header.content_type, "text/plain");
        assert_eq!(parsed[2].header.labels.get("x").unwrap(), "z");
    }

    #[test]
    fn test_label_delta_removal() {
        let mut headers = HeaderMap::new();
        headers.insert(ENTRIES_HEADER, HeaderValue::from_static("entry"));
        headers.insert(START_TS_HEADER, HeaderValue::from_static("0"));
        headers.insert(
            make_batched_header_name(0, 0),
            HeaderValue::from_static("10,text/plain,a=1,b=2"),
        );
        headers.insert(
            make_batched_header_name(0, 5),
            HeaderValue::from_static("5,text/plain,b="),
        );

        let parsed = parse_batched_headers(&headers).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].header.labels.get("a").unwrap(), "1");
        assert_eq!(parsed[0].header.labels.get("b").unwrap(), "2");
        assert_eq!(parsed[1].header.labels.get("a").unwrap(), "1");
        assert!(!parsed[1].header.labels.contains_key("b"));
    }

    #[test]
    fn test_parse_batched_headers_reuse_without_prev_error() {
        let mut headers = HeaderMap::new();
        headers.insert(ENTRIES_HEADER, HeaderValue::from_static("entry"));
        headers.insert(START_TS_HEADER, HeaderValue::from_static("0"));
        headers.insert(
            make_batched_header_name(0, 0),
            HeaderValue::from_static("10"),
        ); // no previous metadata

        let err = parse_batched_headers(&headers).err().unwrap();
        assert_eq!(
            err,
            unprocessable_entity!(
                "Content-type and labels must be provided for the first record of an entry"
            )
        );
    }

    #[test]
    fn test_parse_batched_headers_invalid_index() {
        let mut headers = HeaderMap::new();
        headers.insert(ENTRIES_HEADER, HeaderValue::from_static("entry"));
        headers.insert(START_TS_HEADER, HeaderValue::from_static("0"));
        headers.insert(
            make_batched_header_name(1, 0),
            HeaderValue::from_static("1,text/plain"),
        );

        let err = parse_batched_headers(&headers).err().unwrap();
        assert_eq!(
            err,
            unprocessable_entity!("Invalid header 'x-reduct-1-0': entry index out of range")
        );
    }

    #[test]
    fn test_parse_batched_headers_missing_meta() {
        let mut headers = HeaderMap::new();
        headers.insert(
            make_batched_header_name(0, 0),
            HeaderValue::from_static("1,text/plain"),
        );

        let err = parse_batched_headers(&headers).err().unwrap();
        assert_eq!(
            err,
            unprocessable_entity!("x-reduct-entries header is required")
        );
    }
}
