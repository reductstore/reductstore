// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::error::ReductError;
use crate::Labels;
use http::{HeaderMap, HeaderValue};
use std::collections::HashMap;

pub struct RecordHeader {
    pub content_length: usize,
    pub content_type: String,
    pub labels: Labels,
}

/// Parse a batched header into a content length, content type, and labels.
///
/// # Arguments
///
/// * `header` - The batched header to parse.
///
/// # Returns
///
/// * `content_length` - The content length of the batched header.
/// * `content_type` - The content type of the batched header.
/// * `labels` - The labels of the batched header.
pub fn parse_batched_header(header: &str) -> Result<RecordHeader, ReductError> {
    let (content_length, rest) = header
        .split_once(',')
        .ok_or(ReductError::unprocessable_entity("Invalid batched header"))?;
    let content_length = content_length
        .trim()
        .parse::<usize>()
        .map_err(|_| ReductError::unprocessable_entity("Invalid content length"))?;

    let (content_type, rest) = rest
        .split_once(',')
        .unwrap_or((rest, "application/octet-stream"));

    let content_type = if content_type.is_empty() {
        "application/octet-stream".to_string()
    } else {
        content_type.trim().to_string()
    };

    let mut labels = Labels::new();
    let mut rest = rest.to_string();
    while let Some(pair) = rest.split_once('=') {
        let (key, value) = pair;
        rest = if value.starts_with('\"') {
            let value = value[1..].to_string();
            let (value, rest) = value
                .split_once('\"')
                .ok_or(ReductError::unprocessable_entity("Invalid batched header"))?;
            labels.insert(key.trim().to_string(), value.trim().to_string());
            rest.trim_start_matches(',').trim().to_string()
        } else if let Some(ret) = value.split_once(',') {
            let (value, rest) = ret;
            labels.insert(key.trim().to_string(), value.trim().to_string());
            rest.trim().to_string()
        } else {
            labels.insert(key.trim().to_string(), value.trim().to_string());
            break;
        };
    }

    Ok(RecordHeader {
        content_length,
        content_type,
        labels,
    })
}

pub fn sort_headers_by_name(headers: &HeaderMap) -> Vec<(String, HeaderValue)> {
    let mut sorted_headers: Vec<_> = headers
        .clone()
        .into_iter()
        .map(|(key, value)| (key.unwrap().as_str().to_string(), value))
        .collect();
    sorted_headers.sort_by(|(name1, _), (name2, _)| name1.cmp(name2));
    sorted_headers
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    #[rstest]
    fn test_parse_batched_header_row() {
        let header = "123, text/plain, label1=value1, label2=value2";
        let RecordHeader {
            content_length,
            content_type,
            labels,
        } = parse_batched_header(header).unwrap();
        assert_eq!(content_length, 123);
        assert_eq!(content_type, "text/plain");
        assert_eq!(labels.len(), 2);
        assert_eq!(labels.get("label1"), Some(&"value1".to_string()));
        assert_eq!(labels.get("label2"), Some(&"value2".to_string()));
    }

    #[rstest]
    fn test_parse_batched_header_row_quotes() {
        let header = "123, text/plain, label1=\"[1, 2, 3]\", label2=\"value2\"";
        let RecordHeader {
            content_length,
            content_type,
            labels,
        } = parse_batched_header(header).unwrap();
        assert_eq!(content_length, 123);
        assert_eq!(content_type, "text/plain");
        assert_eq!(labels.len(), 2);
        assert_eq!(labels.get("label1"), Some(&"[1, 2, 3]".to_string()));
        assert_eq!(labels.get("label2"), Some(&"value2".to_string()));
    }

    #[rstest]
    fn test_parse_header_no_labels() {
        let header = "123, text/plain";
        let RecordHeader {
            content_length,
            content_type,
            labels,
        } = parse_batched_header(header).unwrap();
        assert_eq!(content_length, 123);
        assert_eq!(content_type, "text/plain");
        assert_eq!(labels.len(), 0);
    }

    #[rstest]
    #[case("")]
    #[case("xxx")]
    fn test_parse_header_bad_header(#[case] header: &str) {
        let err = parse_batched_header(header).err().unwrap();
        assert_eq!(
            err,
            ReductError::unprocessable_entity("Invalid batched header")
        );
    }
}
