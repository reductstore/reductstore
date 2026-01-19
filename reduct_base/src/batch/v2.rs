// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Batched protocol v2
// -------------------
// Metadata:
//   x-reduct-entries: comma-separated, percent-encoded entry names (index == position)
//   x-reduct-labels (optional): comma-separated, percent-encoded label names (index == position)
//   x-reduct-start-ts: first timestamp in the batch
//   Content is sorted by entry index in ascending order, then by timestamp (start + delta).
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
//     keep their previous value for the entry. When `x-reduct-labels` is provided, the
//     label delta may use indexes instead of names (`<IDX>=<VALUE>`), where the index
//     refers to the position in `x-reduct-labels`.
//   - The first record for an entry must provide content-type and labels when re-use is
//     requested; otherwise defaults apply (content-type defaults to octet-stream, labels to empty).

use crate::batch::v1::RecordHeader;
use crate::error::ReductError;
#[cfg(feature = "io")]
use crate::io::RecordMeta;
use crate::unprocessable_entity;
use crate::Labels;
use http::{HeaderMap, HeaderName, HeaderValue};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

pub const HEADER_PREFIX: &str = "x-reduct-";
pub const ERROR_HEADER_PREFIX: &str = "x-reduct-error-";
pub const ENTRIES_HEADER: &str = "x-reduct-entries";
pub const START_TS_HEADER: &str = "x-reduct-start-ts";
pub const LABELS_HEADER: &str = "x-reduct-labels";

pub const QUERY_ID_HEADER: &str = "x-reduct-query-id";

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

/// Decode a percent-encoded label name.
pub fn decode_label_name(encoded: &str) -> Result<String, ReductError> {
    decode_entry_name(encoded)
        .map_err(|_| unprocessable_entity!("Invalid label encoding in header value: '{}'", encoded))
}

/// Percent-encode a label name.
pub fn encode_label_name(label: &str) -> String {
    encode_entry_name(label)
}

/// Parse the `x-reduct-labels` header containing a comma separated list of percent-encoded labels.
pub fn parse_labels_header(labels: &HeaderValue) -> Result<Vec<String>, ReductError> {
    let labels = labels
        .to_str()
        .map_err(|_| unprocessable_entity!("Invalid labels header"))?;
    if labels.trim().is_empty() {
        return Err(unprocessable_entity!("x-reduct-labels header is empty"));
    }

    labels
        .split(',')
        .map(|label| decode_label_name(label.trim()))
        .collect()
}

/// Keeps label names unique and provides the value for the `x-reduct-labels` header.
#[derive(Debug, Default, Clone)]
pub struct LabelIndex {
    names: Vec<String>,
    lookup: HashMap<String, usize>,
}

impl LabelIndex {
    /// Returns the index of the label name, inserting it if missing.
    pub fn ensure(&mut self, name: &str) -> usize {
        if let Some(idx) = self.lookup.get(name) {
            return *idx;
        }

        let idx = self.names.len();
        self.names.push(name.to_string());
        self.lookup.insert(name.to_string(), idx);
        idx
    }

    /// Returns the header value for `x-reduct-labels` if at least one label was registered.
    pub fn as_header(&self) -> Option<HeaderValue> {
        if self.names.is_empty() {
            return None;
        }

        let encoded = self
            .names
            .iter()
            .map(|name| encode_label_name(name))
            .collect::<Vec<_>>()
            .join(",");
        Some(encoded.parse().unwrap())
    }

    /// Returns the collected label names.
    pub fn names(&self) -> &[String] {
        &self.names
    }
}

/// Build a label delta string for batched protocol v2.
#[cfg(feature = "io")]
pub fn build_label_delta(
    meta: &RecordMeta,
    previous_labels: Option<&Labels>,
    label_index: &mut LabelIndex,
) -> String {
    let mut deltas: Vec<(usize, String)> = Vec::new();

    let format_value = |value: &str| {
        if value.contains(',') {
            format!("\"{}\"", value)
        } else {
            value.to_string()
        }
    };

    if let Some(prev) = previous_labels {
        let mut keys: Vec<String> = prev
            .keys()
            .chain(meta.labels().keys())
            .map(|k| k.to_string())
            .collect();
        keys.sort();
        keys.dedup();

        for key in keys {
            let prev_val = prev.get(&key);
            let curr_val = meta.labels().get(&key);
            match (prev_val, curr_val) {
                (Some(p), Some(c)) if p == c => continue,
                (Some(_), None) => {
                    let idx = label_index.ensure(&key);
                    deltas.push((idx, String::new()))
                }
                (_, Some(c)) => {
                    let idx = label_index.ensure(&key);
                    deltas.push((idx, format_value(c)))
                }
                _ => {}
            }
        }
    } else {
        for (k, v) in meta.labels().iter() {
            let idx = label_index.ensure(k);
            deltas.push((idx, format_value(v)));
        }
    }

    for (k, v) in meta.computed_labels() {
        let idx = label_index.ensure(&format!("@{}", k));
        deltas.push((idx, format_value(v)));
    }

    deltas.sort_by_key(|(idx, _)| *idx);
    deltas
        .into_iter()
        .map(|(idx, value)| format!("{}={}", idx, value))
        .collect::<Vec<_>>()
        .join(",")
}

/// Construct a record header value for batched protocol v2.
#[cfg(feature = "io")]
pub fn make_record_header_value(
    meta: &RecordMeta,
    previous_content_type: Option<&str>,
    previous_labels: Option<&Labels>,
    label_index: &mut LabelIndex,
) -> HeaderValue {
    let mut parts: Vec<String> = vec![meta.content_length().to_string()];

    let mut content_type = String::new();
    match previous_content_type {
        Some(prev) if prev != meta.content_type() => content_type = meta.content_type().to_string(),
        None => content_type = meta.content_type().to_string(),
        _ => {}
    }

    let labels_delta = build_label_delta(meta, previous_labels, label_index);
    let has_labels = !labels_delta.is_empty();

    if !content_type.is_empty() || has_labels {
        parts.push(content_type);
    }

    if has_labels {
        parts.push(labels_delta);
    }

    parts.join(",").parse().unwrap()
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

pub fn make_error_batched_header(
    entry_index: usize,
    start_ts: u64,
    time_delta: u64,
    err: &ReductError,
) -> (HeaderName, HeaderValue) {
    let name = HeaderName::from_str(&format!(
        "{}{}-{}",
        ERROR_HEADER_PREFIX,
        entry_index,
        time_delta + start_ts
    ))
    .expect("Entry index and time delta must produce a valid header name");
    let value = HeaderValue::from_str(&format!("{},{}", err.status(), err.message()))
        .expect("Status code and message must produce a valid header value");
    (name, value)
}

pub fn make_entries_header(entries: &[String]) -> HeaderValue {
    let encoded = entries
        .iter()
        .map(|entry| encode_entry_name(entry))
        .collect::<Vec<_>>()
        .join(",");
    encoded.parse().unwrap()
}

fn parse_start_timestamp_internal(headers: &HeaderMap) -> Result<u64, ReductError> {
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

pub fn parse_start_timestamp(headers: &HeaderMap) -> Result<u64, ReductError> {
    parse_start_timestamp_internal(headers)
}

pub fn parse_entries(headers: &HeaderMap) -> Result<Vec<String>, ReductError> {
    headers
        .get(ENTRIES_HEADER)
        .ok_or(unprocessable_entity!("x-reduct-entries header is required"))
        .and_then(parse_entries_header)
}

pub fn parse_labels(headers: &HeaderMap) -> Result<Option<Vec<String>>, ReductError> {
    match headers.get(LABELS_HEADER) {
        None => Ok(None),
        Some(labels) => parse_labels_header(labels).map(Some),
    }
}

pub fn resolve_label_name<'a>(
    raw: &'a str,
    label_names: Option<&Vec<String>>,
) -> Result<String, ReductError> {
    if let (Some(label_names), Ok(idx)) = (label_names, raw.parse::<usize>()) {
        return label_names
            .get(idx)
            .cloned()
            .ok_or_else(|| unprocessable_entity!("Label index '{}' is out of range", raw));
    }

    if raw.starts_with('@') {
        return Err(unprocessable_entity!(
            "Label names must not start with '@': reserved for computed labels",
        ));
    }

    Ok(raw.to_string())
}

fn apply_label_delta(
    raw_labels: &str,
    base: &Labels,
    label_names: Option<&Vec<String>>,
) -> Result<Labels, ReductError> {
    let mut labels = base.clone();
    for (key, value) in parse_label_delta_ops(raw_labels, label_names)? {
        match value {
            Some(value) => {
                labels.insert(key.to_string(), value);
            }
            None => {
                labels.remove(&key);
            }
        }
    }

    Ok(labels)
}

fn parse_label_delta_ops(
    raw_labels: &str,
    label_names: Option<&Vec<String>>,
) -> Result<Vec<(String, Option<String>)>, ReductError> {
    let mut ops = Vec::new();
    let mut rest = raw_labels.trim().to_string();

    if rest.is_empty() {
        return Ok(ops);
    }

    loop {
        let (raw_key, value_part) = rest
            .split_once('=')
            .ok_or_else(|| unprocessable_entity!("Invalid batched header"))?;
        let key = resolve_label_name(raw_key.trim(), label_names)?;

        let (value, next_rest) = if value_part.starts_with('\"') {
            let value_part = &value_part[1..];
            let (value, rest) = value_part
                .split_once('\"')
                .ok_or_else(|| unprocessable_entity!("Invalid batched header"))?;
            (
                value.trim().to_string(),
                rest.trim_start_matches(',').trim().to_string(),
            )
        } else if let Some((value, rest)) = value_part.split_once(',') {
            (value.trim().to_string(), rest.trim().to_string())
        } else {
            (value_part.trim().to_string(), String::new())
        };

        let value = if value.is_empty() { None } else { Some(value) };
        ops.push((key, value));

        if next_rest.is_empty() {
            break;
        }
        rest = next_rest;
    }

    Ok(ops)
}

pub fn parse_label_delta(
    raw_labels: &str,
    label_names: Option<&Vec<String>>,
) -> Result<(Labels, HashSet<String>), ReductError> {
    let mut updates = Labels::new();
    let mut remove = HashSet::new();

    for (key, value) in parse_label_delta_ops(raw_labels, label_names)? {
        match value {
            Some(value) => {
                updates.insert(key, value);
            }
            None => {
                remove.insert(key);
            }
        }
    }

    Ok((updates, remove))
}

fn parse_record_header_with_defaults(
    raw: &str,
    previous: Option<&RecordHeader>,
    label_names: Option<&Vec<String>>,
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
            label_names,
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
/// Headers are sorted by entry index, then timestamp (start + delta).
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

    parsed_headers.sort_by(|(idx_a, delta_a, _), (idx_b, delta_b, _)| {
        idx_a.cmp(idx_b).then_with(|| delta_a.cmp(delta_b))
    });

    Ok(parsed_headers)
}

/// Parse and sort v2 batched headers including record metadata.
pub fn parse_batched_headers(headers: &HeaderMap) -> Result<Vec<EntryRecordHeader>, ReductError> {
    let entries = parse_entries(headers)?;
    let start_ts = parse_start_timestamp(headers)?;
    let label_names = parse_labels(headers)?;
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

        let header = parse_record_header_with_defaults(
            raw_value,
            last_header_per_entry.get(entry),
            label_names.as_ref(),
        )?;
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
    fn test_parse_labels_header_roundtrip() {
        let value = HeaderValue::from_str("label-1,foo%2Fbar").unwrap();
        let labels = parse_labels_header(&value).unwrap();
        assert_eq!(labels, vec!["label-1".to_string(), "foo/bar".to_string()]);
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
        headers.insert(
            make_batched_header_name(1, 3),
            HeaderValue::from_static("1,text/plain"),
        );

        let parsed = sort_headers_by_entry_and_time(&headers).unwrap();
        assert_eq!(parsed.len(), 4);
        assert_eq!(parsed[0].0, 0);
        assert_eq!(parsed[0].1, 2);
        assert_eq!(parsed[1].0, 0);
        assert_eq!(parsed[1].1, 3);
        assert_eq!(parsed[2].0, 1);
        assert_eq!(parsed[2].1, 3);
        assert_eq!(parsed[3].0, 1);
        assert_eq!(parsed[3].1, 5);
    }

    #[test]
    fn test_parse_batched_headers_with_values() {
        let mut headers = HeaderMap::new();
        headers.insert(ENTRIES_HEADER, HeaderValue::from_static("entry,ro%2Ftopic"));
        headers.insert(START_TS_HEADER, HeaderValue::from_static("1000"));
        headers.insert(LABELS_HEADER, HeaderValue::from_static("label"));
        headers.insert(
            make_batched_header_name(1, 15),
            HeaderValue::from_static("3,text/plain,0=z"),
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
        assert_eq!(parsed[1].header.labels.get("label").unwrap(), "z");
    }

    #[test]
    fn test_parse_batched_headers_reuse_metadata() {
        let mut headers = HeaderMap::new();
        headers.insert(ENTRIES_HEADER, HeaderValue::from_static("entry"));
        headers.insert(START_TS_HEADER, HeaderValue::from_static("0"));
        headers.insert(LABELS_HEADER, HeaderValue::from_static("x"));
        headers.insert(
            make_batched_header_name(0, 0),
            HeaderValue::from_static("10,text/plain,0=y"),
        );
        headers.insert(
            make_batched_header_name(0, 5),
            HeaderValue::from_static("2"),
        ); // reuse type + labels
        headers.insert(
            make_batched_header_name(0, 10),
            HeaderValue::from_static("3,,0=z"),
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
    fn test_parse_batched_headers_with_label_indexes() {
        let mut headers = HeaderMap::new();
        headers.insert(ENTRIES_HEADER, HeaderValue::from_static("entry"));
        headers.insert(START_TS_HEADER, HeaderValue::from_static("0"));
        headers.insert(LABELS_HEADER, HeaderValue::from_static("a,b"));

        headers.insert(
            make_batched_header_name(0, 0),
            HeaderValue::from_static("10,text/plain,0=1,1=2"),
        );
        headers.insert(
            make_batched_header_name(0, 5),
            HeaderValue::from_static("2,,0="), // remove label a
        );

        let parsed = parse_batched_headers(&headers).unwrap();
        assert_eq!(parsed[0].header.labels.get("a").unwrap(), "1");
        assert_eq!(parsed[0].header.labels.get("b").unwrap(), "2");
        assert!(!parsed[1].header.labels.contains_key("a"));
        assert_eq!(parsed[1].header.labels.get("b").unwrap(), "2");
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

    #[test]
    fn test_parse_label_delta_updates_and_removals() {
        let label_names = vec!["a".to_string(), "b".to_string()];
        let (updates, remove) =
            parse_label_delta("0=one,1=,c=\"3,4\"", Some(&label_names)).unwrap();

        assert_eq!(updates.get("a").unwrap(), "one");
        assert_eq!(updates.get("c").unwrap(), "3,4");
        assert!(remove.contains("b"));
        assert_eq!(remove.len(), 1);
    }

    #[test]
    fn test_resolve_label_name_reserved_prefix() {
        let err = resolve_label_name("@cpu", None).err().unwrap();
        assert_eq!(
            err,
            unprocessable_entity!(
                "Label names must not start with '@': reserved for computed labels",
            )
        );
    }

    #[test]
    fn test_resolve_label_name_index_out_of_range() {
        let label_names = vec!["a".to_string()];
        let err = resolve_label_name("2", Some(&label_names)).err().unwrap();
        assert_eq!(
            err,
            unprocessable_entity!("Label index '2' is out of range")
        );
    }

    #[test]
    fn test_make_error_batched_header() {
        let err = unprocessable_entity!("broken");
        let (name, value) = make_error_batched_header(2, 10, &err);

        assert_eq!(name.as_str(), "x-reduct-error-2-10");
        assert_eq!(
            value.to_str().unwrap(),
            format!("{},{}", err.status(), err.message())
        );
    }

    #[cfg(feature = "io")]
    mod io_tests {
        use super::*;
        use std::collections::HashMap;

        fn build_meta(
            labels: &[(&str, &str)],
            computed: &[(&str, &str)],
            content_type: &str,
            content_length: u64,
        ) -> RecordMeta {
            let mut label_map = HashMap::new();
            for (key, value) in labels {
                label_map.insert((*key).to_string(), (*value).to_string());
            }

            let mut computed_map = HashMap::new();
            for (key, value) in computed {
                computed_map.insert((*key).to_string(), (*value).to_string());
            }

            RecordMeta::builder()
                .labels(label_map)
                .computed_labels(computed_map)
                .content_type(content_type.to_string())
                .content_length(content_length)
                .build()
        }

        #[test]
        fn test_build_label_delta_with_previous_and_computed() {
            let mut label_index = LabelIndex::default();
            label_index.ensure("a");
            label_index.ensure("b");
            label_index.ensure("c");
            label_index.ensure("@cpu");

            let mut previous = Labels::new();
            previous.insert("a".to_string(), "1".to_string());
            previous.insert("b".to_string(), "2".to_string());

            let meta = build_meta(&[("a", "1"), ("c", "3,4")], &[("cpu", "10")], "text", 1);

            let delta = build_label_delta(&meta, Some(&previous), &mut label_index);
            assert_eq!(delta, "1=,2=\"3,4\",3=10");
        }

        #[test]
        fn test_make_record_header_value_reuse_metadata() {
            let mut label_index = LabelIndex::default();
            label_index.ensure("a");

            let mut previous = Labels::new();
            previous.insert("a".to_string(), "1".to_string());

            let meta = build_meta(&[("a", "1")], &[], "text/plain", 8);
            let value = make_record_header_value(
                &meta,
                Some("text/plain"),
                Some(&previous),
                &mut label_index,
            );

            assert_eq!(value.to_str().unwrap(), "8");
        }

        #[test]
        fn test_make_record_header_value_with_label_delta() {
            let mut label_index = LabelIndex::default();
            label_index.ensure("a");

            let previous = Labels::new();
            let meta = build_meta(&[("a", "1")], &[], "text/plain", 10);
            let value = make_record_header_value(
                &meta,
                Some("text/plain"),
                Some(&previous),
                &mut label_index,
            );

            assert_eq!(value.to_str().unwrap(), "10,,0=1");
        }
    }
}
