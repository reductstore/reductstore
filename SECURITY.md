# Security Policy

This repository follows the ReductStore coordinated vulnerability disclosure policy maintained by
ReductSoftware UG.

Canonical policy:

- https://github.com/reductstore/security/blob/main/docs/policies/vulnerability-disclosure.md
- Tracking issue: https://github.com/reductstore/security/issues/36

## Supported Versions

Security updates are provided for the latest stable release of `reductstore/reductstore` unless stated otherwise in
project release notes or maintenance documentation. If a security update is published, upgrade to
the latest stable version as soon as practical.

## Reporting a Vulnerability

Do not report suspected vulnerabilities in public GitHub issues, pull requests, discussions, chat
rooms, or social media.

Preferred reporting channel:

- Use GitHub Security Advisories for this repository:
  https://github.com/reductstore/reductstore/security/advisories/new

Fallback reporting channel:

- Email security@reduct.store if GitHub Security Advisories are unavailable or if you cannot use
  the private advisory workflow.

Please include:

- Affected version, commit, package, image, or artifact
- Vulnerability type and expected security impact
- Reproduction steps, proof of concept, logs, screenshots, or test case
- Required privileges, configuration, or deployment assumptions
- Whether the issue is already public or appears to be actively exploited
- Your contact details and any disclosure timeline constraints

Do not include secrets, customer data, or data from systems you do not own or do not have
permission to test.

## Response Commitments

ReductStore maintainers will:

- Acknowledge receipt within 48 hours.
- Keep the report private while it is being validated and remediated.
- Coordinate validation, remediation, release, and disclosure with the relevant maintainers.
- Provide meaningful status updates when there is a material change, fix, or disclosure decision.

## Coordinated Disclosure

The default coordinated disclosure window is 90 days from acknowledgement of the report, unless a
shorter or longer timeline is required due to active exploitation, public disclosure, multi-project
coordination, or mutual agreement with the reporter.

## Safe Harbor

ReductSoftware UG will not pursue legal action or request law-enforcement action against
researchers who make a good-faith effort to follow the coordinated vulnerability disclosure policy.

This safe harbor does not authorize testing against third-party systems, customer environments, or
services not owned or operated by ReductSoftware UG.
