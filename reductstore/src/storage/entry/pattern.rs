// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

pub(crate) fn entry_matches_pattern(entry: &str, pattern: &str) -> bool {
    let pattern = pattern.trim_start_matches('/');

    if !pattern.contains('*') {
        return entry == pattern;
    }

    if !pattern.contains('/') {
        if let Some(prefix) = pattern.strip_suffix('*') {
            return entry.starts_with(prefix);
        }
    }

    let entry_parts: Vec<&str> = entry.split('/').collect();
    let pattern_parts: Vec<&str> = pattern.split('/').collect();

    fn segment_matches(entry: &str, pattern: &str) -> bool {
        if pattern == "**" {
            return true;
        }

        let mut rest = entry;
        let mut parts = pattern.split('*').peekable();

        if let Some(first) = parts.next() {
            if !first.is_empty() {
                let Some(stripped) = rest.strip_prefix(first) else {
                    return false;
                };
                rest = stripped;
            }
        }

        while let Some(part) = parts.next() {
            if part.is_empty() {
                continue;
            }

            if parts.peek().is_none() {
                return rest.ends_with(part);
            }

            let Some(index) = rest.find(part) else {
                return false;
            };
            rest = &rest[index + part.len()..];
        }

        pattern.ends_with('*') || rest.is_empty()
    }

    fn matches_from(entry_parts: &[&str], pattern_parts: &[&str]) -> bool {
        match pattern_parts.split_first() {
            None => entry_parts.is_empty(),
            Some((&"**", tail)) => {
                matches_from(entry_parts, tail)
                    || (!entry_parts.is_empty() && matches_from(&entry_parts[1..], pattern_parts))
            }
            Some((pattern, tail)) => {
                !entry_parts.is_empty()
                    && segment_matches(entry_parts[0], pattern)
                    && matches_from(&entry_parts[1..], tail)
            }
        }
    }

    matches_from(&entry_parts, &pattern_parts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("acc-a", "acc-*", true)]
    #[case("acc-a/sub-entry", "acc-*", true)]
    #[case("other", "acc-*", false)]
    #[case("a/x/b", "/a/*/b", true)]
    #[case("a/y/b", "/a/*/b", true)]
    #[case("a/x/d/b", "/a/*/b", false)]
    #[case("a/x/b", "/a/**/b", true)]
    #[case("a/x/d/b", "/a/**/b", true)]
    #[case("a/private/x/b", "/a/private/**", true)]
    #[case("a/public/x/b", "/a/private/**", false)]
    #[case("a/x/b", "/**/**/", false)]
    fn matches_entry_patterns(#[case] entry: &str, #[case] pattern: &str, #[case] expected: bool) {
        assert_eq!(entry_matches_pattern(entry, pattern), expected);
    }
}
