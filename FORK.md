# Stratosphere Wings

A fork of [Wings](https://github.com/pterodactyl/wings) for the Stratosphere panel. It is Wings, plus one thing: **the panel can give
a node its https certificate**, so a node needs no certbot, no files and no restart.

## What is different

- `GET /api/certificates` says which certificate the node serves (names, end date, fingerprint, where it came from), never the key.
- `POST /api/certificates` takes `{"certificate": "<PEM chain>", "key": "<PEM>"}` from the panel (same node token as every other call),
  checks that the pair works and is not expired, keeps it in `<root_directory>/.panel-tls` (key `0600`, directory `0700`) and **starts using
  it for the next connection**. A pair that does not work changes nothing.
- A node that is **not** set to use https (`api.ssl.enabled: false`) now answers both http and https on its port: the first byte of a TLS
  handshake tells them apart. As soon as the panel pushed a certificate, https works, without touching the machine. Plain http keeps
  working exactly as before.
- A node that **is** set to use https prefers the pushed certificate, then the files of `api.ssl.cert` / `api.ssl.key`, and when neither exists
  (a node that was just set up) a temporary self signed one, so it starts instead of stopping; the panel then gives it the real one.

Everything else is unchanged, including `--auto-tls` (Let's Encrypt by Wings itself).

## Setting a node up with it

1. Install this Wings in place of the official one (same config file, same service): `wings_linux_amd64` / `wings_linux_arm64` from the
   [nightly release](https://github.com/kokofixcomputers/stratwings/releases/tag/nightly) (or `make build`), and restart it once.
2. In the panel: **Settings > Certificates**: add the certificate (fullchain) and key of the node's name (a wildcard works).
3. In the panel: the node, **Certificate** tab: choose it (or "the one that covers this node") and press **Enable https on this node**.

The panel renews by itself after that: a replaced certificate is given to every node that uses it within the hour.

## Build

`go build` needs Linux system calls, so build for Linux: `GOOS=linux GOARCH=amd64 go build -o wings .`. The tests of the new package run
anywhere: `go test ./internal/certstore`. The rest of the tests of Wings need Linux, as before.

## Releases

The `nightly` release is built by `.github/workflows/nightly.yaml` on every push to `develop` and every night: it runs the tests, builds Linux
binaries (amd64, arm64) with checksums, and moves the `nightly` tag to the commit it built.
