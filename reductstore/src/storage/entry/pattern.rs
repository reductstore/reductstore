// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

/// Matches a name against a pattern list where a leading `!` marks an exclusion.
pub(crate) fn matches_patterns(name: &str, patterns: &[String]) -> bool {
    let mut includes = patterns
        .iter()
        .filter(|pattern| !is_exclusion(pattern))
        .peekable();
    let included =
        includes.peek().is_none() || includes.any(|pattern| entry_matches_pattern(name, pattern));

    included
        && !patterns
            .iter()
            .filter(|pattern| is_exclusion(pattern))
            .any(|pattern| entry_matches_pattern(name, &pattern[1..]))
}

fn is_exclusion(pattern: &str) -> bool {
    pattern.starts_with('!') && pattern.len() > 1
}

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
    #[case("a/b", "/a/**", true)]
    #[case("a/private/x/b", "/a/private/**", true)]
    #[case("a/public/x/b", "/a/private/**", false)]
    #[case("a/x/b", "/**/**/", false)]
    #[case("sensor-alpha-temp/b", "/camera-*/b", false)]
    #[case("sensor-alpha-temp", "sensor-*temp", true)]
    #[case("sensor-alpha-temp", "sensor-*humidity", false)]
    #[case("a/sensor-alpha-temp/b", "/a/sensor-*alpha-*/b", true)]
    #[case("a/sensor-alpha-temp/b", "/a/sensor-*beta-*/b", false)]
    #[case("sensor-alpha-temp", "sensor-*", true)]
    #[case("a/sensor-alpha-temp/b", "/a/*alpha*/b", true)]
    fn matches_entry_patterns(#[case] entry: &str, #[case] pattern: &str, #[case] expected: bool) {
        assert_eq!(entry_matches_pattern(entry, pattern), expected);
    }

    #[rstest]
    #[case("site_a", &[], true)]
    #[case("site_a", &["site_*"], true)]
    #[case("cell_a", &["site_*"], false)]
    #[case("cell_a", &["site_*", "cell_*"], true)]
    #[case("site_test", &["site_*", "!site_test*"], false)]
    #[case("site_prod", &["site_*", "!site_test*"], true)]
    #[case("anything", &["!secret_*"], true)]
    #[case("secret_a", &["!secret_*"], false)]
    #[case("!", &["!"], true)]
    fn matches_pattern_lists(
        #[case] name: &str,
        #[case] patterns: &[&str],
        #[case] expected: bool,
    ) {
        let patterns: Vec<String> = patterns.iter().map(|p| p.to_string()).collect();
        assert_eq!(matches_patterns(name, &patterns), expected);
    }
}
