//! AWS Signature Version 4 signing for HTTP requests.
//!
//! Implements the [AWS Signature Version 4](https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html)
//! algorithm for signing API requests. Currently used by the Redshift provider
//! to call the `GetClusterCredentials` API for IAM authentication.
//!
//! ## Scope
//!
//! This module provides the signing primitives needed to authenticate with AWS
//! APIs that use query-string (GET) requests. It does not aim to be a
//! general-purpose AWS SDK.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// AWS credentials for signing requests.
#[derive(Debug, Clone)]
pub struct AwsCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
}

/// Build the `Authorization` header value for an AWS Signature V4 signed request.
///
/// Implements the [AWS Signature Version 4 signing process](https://docs.aws.amazon.com/general/latest/gr/sigv4_signing.html)
/// for query-string API requests (GET with no body).
///
/// # Arguments
///
/// * `method` - HTTP method (e.g., "GET").
/// * `host` - The API endpoint host (e.g., "redshift.us-east-1.amazonaws.com").
/// * `path` - The URL path (e.g., "/").
/// * `query_string` - The canonical query string (parameters sorted by key, URL-encoded).
/// * `region` - AWS region (e.g., "us-east-1").
/// * `service` - AWS service name (e.g., "redshift").
/// * `credentials` - AWS access key ID and secret access key.
/// * `datetime` - ISO 8601 basic format timestamp (e.g., "20190825T160000Z").
/// * `datestamp` - Date portion of the timestamp (e.g., "20190825").
///
/// # Returns
///
/// A tuple of `(authorization_header_value, signed_headers_string)`.
#[allow(clippy::too_many_arguments)]
pub fn sign_request(
    method: &str,
    host: &str,
    path: &str,
    query_string: &str,
    region: &str,
    service: &str,
    credentials: &AwsCredentials,
    datetime: &str,
    datestamp: &str,
) -> String {
    let signed_headers = "host;x-amz-date";

    // Step 1: Create canonical request
    // For GET requests with no body, the payload hash is the SHA256 of empty string.
    let payload_hash = hex_sha256(b"");

    let canonical_headers = format!("host:{host}\nx-amz-date:{datetime}\n");

    let canonical_request = format!(
        "{method}\n{path}\n{query_string}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    );

    // Step 2: Create string to sign
    let credential_scope = format!("{datestamp}/{region}/{service}/aws4_request");
    let canonical_request_hash = hex_sha256(canonical_request.as_bytes());

    let string_to_sign =
        format!("AWS4-HMAC-SHA256\n{datetime}\n{credential_scope}\n{canonical_request_hash}");

    // Step 3: Calculate signing key
    let signing_key =
        derive_signing_key(&credentials.secret_access_key, datestamp, region, service);

    // Step 4: Calculate signature
    let signature = hex_hmac_sha256(&signing_key, string_to_sign.as_bytes());

    // Step 5: Build Authorization header
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}",
        credentials.access_key_id
    )
}

/// Compute the SHA-256 hash of data and return it as a lowercase hex string.
fn hex_sha256(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    hex::encode(result)
}

/// Compute HMAC-SHA256 and return the raw bytes.
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts keys of any size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Compute HMAC-SHA256 and return the result as a lowercase hex string.
fn hex_hmac_sha256(key: &[u8], data: &[u8]) -> String {
    hex::encode(hmac_sha256(key, data))
}

/// Derive the AWS Signature V4 signing key.
///
/// ```text
/// kDate    = HMAC("AWS4" + secret, datestamp)
/// kRegion  = HMAC(kDate, region)
/// kService = HMAC(kRegion, service)
/// kSigning = HMAC(kService, "aws4_request")
/// ```
fn derive_signing_key(
    secret_access_key: &str,
    datestamp: &str,
    region: &str,
    service: &str,
) -> Vec<u8> {
    let k_secret = format!("AWS4{secret_access_key}");
    let k_date = hmac_sha256(k_secret.as_bytes(), datestamp.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

/// URL-encode a string using AWS-style percent encoding.
///
/// AWS SigV4 requires RFC 3986 encoding where all characters except
/// unreserved characters (A-Z, a-z, 0-9, `-`, `.`, `_`, `~`) are
/// percent-encoded.
pub fn aws_url_encode(input: &str) -> String {
    let mut encoded = String::with_capacity(input.len() * 2);
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(byte as char);
            }
            _ => {
                encoded.push_str(&format!("%{byte:02X}"));
            }
        }
    }
    encoded
}

/// Build a canonical query string from key-value pairs.
///
/// Sorts parameters by key (ascending), URL-encodes both keys and values,
/// and joins with `&`.
pub fn build_canonical_query_string(params: &[(&str, &str)]) -> String {
    let mut sorted_params: Vec<_> = params
        .iter()
        .map(|(k, v)| (aws_url_encode(k), aws_url_encode(v)))
        .collect();
    sorted_params.sort();
    sorted_params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_sha256_empty_string() {
        // SHA-256 of empty string is a well-known constant
        assert_eq!(
            hex_sha256(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn hex_sha256_hello() {
        // SHA-256("hello") = well-known value
        assert_eq!(
            hex_sha256(b"hello"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn hmac_sha256_basic() {
        let result = hex_hmac_sha256(b"key", b"message");
        // HMAC-SHA256("key", "message") is a well-known test vector
        assert_eq!(
            result,
            "6e9ef29b75fffc5b7abae527d58fdadb2fe42e7219011976917343065f58ed4a"
        );
    }

    #[test]
    fn derive_signing_key_produces_expected_length() {
        let key = derive_signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20120215",
            "us-east-1",
            "iam",
        );
        // Signing key should be 32 bytes (SHA-256 output)
        assert_eq!(key.len(), 32);
    }

    /// Test vector from the AWS SigV4 documentation.
    ///
    /// See: <https://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html>
    #[test]
    fn derive_signing_key_aws_test_vector() {
        let key = derive_signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20120215",
            "us-east-1",
            "iam",
        );
        let expected = [
            0xf4, 0x78, 0x0e, 0x2d, 0x9f, 0x65, 0xfa, 0x89, 0x5f, 0x9c, 0x67, 0xb3, 0x2c, 0xe1,
            0xba, 0xf0, 0xb0, 0xd8, 0xa4, 0x35, 0x05, 0xa0, 0x00, 0xa1, 0xa9, 0xe0, 0x90, 0xd4,
            0x14, 0xdb, 0x40, 0x4d,
        ];
        assert_eq!(key, expected);
    }

    #[test]
    fn aws_url_encode_unreserved_chars() {
        assert_eq!(aws_url_encode("AZaz09-._~"), "AZaz09-._~");
    }

    #[test]
    fn aws_url_encode_special_chars() {
        assert_eq!(aws_url_encode("hello world"), "hello%20world");
        assert_eq!(aws_url_encode("a+b"), "a%2Bb");
        assert_eq!(aws_url_encode("foo/bar"), "foo%2Fbar");
        assert_eq!(aws_url_encode("key=value"), "key%3Dvalue");
    }

    #[test]
    fn aws_url_encode_empty_string() {
        assert_eq!(aws_url_encode(""), "");
    }

    #[test]
    fn build_canonical_query_string_sorts_by_key() {
        let params = [("Zebra", "1"), ("Apple", "2"), ("Mango", "3")];
        let result = build_canonical_query_string(&params);
        assert_eq!(result, "Apple=2&Mango=3&Zebra=1");
    }

    #[test]
    fn build_canonical_query_string_encodes_values() {
        let params = [
            ("Action", "GetClusterCredentials"),
            ("DbUser", "admin user"),
        ];
        let result = build_canonical_query_string(&params);
        assert_eq!(result, "Action=GetClusterCredentials&DbUser=admin%20user");
    }

    #[test]
    fn build_canonical_query_string_empty() {
        let params: [(&str, &str); 0] = [];
        let result = build_canonical_query_string(&params);
        assert_eq!(result, "");
    }

    #[test]
    fn sign_request_produces_expected_format() {
        let auth = sign_request(
            "GET",
            "redshift.us-east-1.amazonaws.com",
            "/",
            "Action=GetClusterCredentials&ClusterIdentifier=mycluster&DbUser=admin&Version=2012-12-01",
            "us-east-1",
            "redshift",
            &AwsCredentials {
                access_key_id: "AKIAIOSFODNN7EXAMPLE".into(),
                secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into(),
            },
            "20190825T160000Z",
            "20190825",
        );

        // Verify the format is correct
        assert!(auth.starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20190825/us-east-1/redshift/aws4_request, SignedHeaders=host;x-amz-date, Signature="));
        // Signature should be 64 hex characters
        let sig_start = auth.rfind("Signature=").unwrap() + "Signature=".len();
        let signature = &auth[sig_start..];
        assert_eq!(
            signature.len(),
            64,
            "Signature should be 64 hex chars, got: {signature}"
        );
        assert!(
            signature.chars().all(|c| c.is_ascii_hexdigit()),
            "Signature should be hex: {signature}"
        );
    }

    #[test]
    fn sign_request_deterministic() {
        // Same inputs should produce the same signature
        let params = (
            "GET",
            "redshift.us-east-1.amazonaws.com",
            "/",
            "Action=GetClusterCredentials",
            "us-east-1",
            "redshift",
            AwsCredentials {
                access_key_id: "AKID".into(),
                secret_access_key: "SECRET".into(),
            },
            "20190825T160000Z",
            "20190825",
        );

        let auth1 = sign_request(
            params.0, params.1, params.2, params.3, params.4, params.5, &params.6, params.7,
            params.8,
        );
        let auth2 = sign_request(
            params.0, params.1, params.2, params.3, params.4, params.5, &params.6, params.7,
            params.8,
        );
        assert_eq!(auth1, auth2);
    }
}
