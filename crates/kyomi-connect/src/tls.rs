/// Select a provider before any TLS client is created. Datasource dependencies
/// enable both Rustls providers, so Rustls cannot choose one automatically.
pub(crate) fn install_crypto_provider() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Rustls crypto provider must be installed once at startup");
}

#[cfg(test)]
#[path = "tls_tests.rs"]
mod tests;
