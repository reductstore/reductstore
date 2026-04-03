// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{HeaderMap, Request};
use std::net::{IpAddr, SocketAddr};

pub(super) fn client_ip_from_request(request: &Request<Body>) -> Option<IpAddr> {
    let peer_ip = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|connect_info| connect_info.0.ip())?;

    if is_trusted_proxy(peer_ip) {
        Some(client_ip_from_forward_headers(request.headers()).unwrap_or(peer_ip))
    } else {
        Some(peer_ip)
    }
}

fn is_trusted_proxy(ip: IpAddr) -> bool {
    ip.is_loopback()
}

fn client_ip_from_forward_headers(headers: &HeaderMap) -> Option<IpAddr> {
    if let Some(value) = headers.get("forwarded").and_then(|v| v.to_str().ok()) {
        if let Some(ip) = parse_forwarded_for(value) {
            return Some(ip);
        }
    }

    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(parse_x_forwarded_for)
}

pub(super) fn parse_x_forwarded_for(value: &str) -> Option<IpAddr> {
    value
        .split(',')
        .next()
        .map(str::trim)
        .and_then(|ip| ip.parse::<IpAddr>().ok())
}

pub(super) fn parse_forwarded_for(value: &str) -> Option<IpAddr> {
    for part in value.split(';') {
        let mut kv = part.splitn(2, '=');
        let key = kv.next()?.trim();
        let val = kv.next()?.trim();
        if key.eq_ignore_ascii_case("for") {
            let token = val.trim_matches('"').trim();
            let token = token
                .strip_prefix('[')
                .and_then(|t| t.strip_suffix(']'))
                .unwrap_or(token);

            if let Ok(ip) = token.parse::<IpAddr>() {
                return Some(ip);
            }

            if token.matches(':').count() == 1 {
                if let Some((ip, _port)) = token.split_once(':') {
                    if let Ok(ip) = ip.parse::<IpAddr>() {
                        return Some(ip);
                    }
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_forwarded_for_handles_common_cases() {
        assert_eq!(
            parse_forwarded_for("for=203.0.113.10").map(|ip| ip.to_string()),
            Some("203.0.113.10".to_string())
        );
        assert_eq!(
            parse_forwarded_for("for=203.0.113.10:443").map(|ip| ip.to_string()),
            Some("203.0.113.10".to_string())
        );
        assert_eq!(
            parse_forwarded_for("for=\"[2001:db8::1]\"").map(|ip| ip.to_string()),
            Some("2001:db8::1".to_string())
        );
        assert_eq!(parse_forwarded_for("proto=https"), None);
    }

    #[test]
    fn parse_x_forwarded_for_handles_common_cases() {
        assert_eq!(
            parse_x_forwarded_for("198.51.100.7, 203.0.113.1").map(|ip| ip.to_string()),
            Some("198.51.100.7".to_string())
        );
        assert_eq!(parse_x_forwarded_for("bad-ip"), None);
    }

    #[test]
    fn client_ip_resolution_from_request() {
        let request = Request::get("/x").body(Body::empty()).unwrap();
        assert_eq!(client_ip_from_request(&request), None);

        let mut request = Request::get("/x").body(Body::empty()).unwrap();
        request.extensions_mut().insert(ConnectInfo(
            "198.51.100.20:80".parse::<SocketAddr>().unwrap(),
        ));
        assert_eq!(
            client_ip_from_request(&request).map(|ip| ip.to_string()),
            Some("198.51.100.20".to_string())
        );

        let mut request = Request::get("/x")
            .header("x-forwarded-for", "203.0.113.3, 198.51.100.4")
            .body(Body::empty())
            .unwrap();
        request
            .extensions_mut()
            .insert(ConnectInfo("127.0.0.1:80".parse::<SocketAddr>().unwrap()));
        assert_eq!(
            client_ip_from_request(&request).map(|ip| ip.to_string()),
            Some("203.0.113.3".to_string())
        );
    }
}
