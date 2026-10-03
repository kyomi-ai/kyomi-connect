These certificates and the private key are synthetic local test fixtures. The
server certificate is signed by the test CA, has only `DNS:localhost` in its SAN,
and expires in 2126. The CA private key is discarded; none of these fixtures are
used by the production binary or trusted outside the regression test.
