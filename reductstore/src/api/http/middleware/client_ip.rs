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
