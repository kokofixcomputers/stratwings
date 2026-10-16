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

## Updates and installing

* `POST /api/system/update` (the panel's **Update** buttons): downloads `wings_linux_<arch>` and `checksums.txt` of the release in
  `WINGS_UPDATE_URL` (default: the nightly release), checks the hash, runs `version` of the new binary, keeps the old one as `wings.old`
  and restarts in place. `wings.updating` marks a binary on trial: three starts without staying up a minute put `wings.old` back.
* `wings install <panel-url> <token> [node-id]` (root): fetches the configuration, writes `/etc/systemd/system/wings.service` if there is
  none, and runs `systemctl enable --now wings`.

## Server importer

`POST /api/servers/<uuid>/files/import/test` and `POST /api/servers/<uuid>/files/import` (the panel's Importer page): connect to an SFTP or FTP(S)
server and copy a folder of it into the server. The node reads from the remote server itself, nothing goes through the panel.

* `test` connects, lists the first 100 entries of the folder and returns the SSH host key fingerprint, which the panel shows and sends back
  with `import` so a different server is refused.
* `import` streams one JSON line per step: `connected`, `listing` (files, folders, bytes), one `file` per copied file
  (`done`, `total`, `path`, `ok`, `error`, `bytes`), then `finished` or `error`. SFTP copies four files at a time, FTP one.
* Links and special files on the remote server are skipped, names with `/`, `\`, `..` or NUL are skipped. Files are written through the
  server's file system: its disk limit counts and its denylist applies. A full disk stops the import.
* Addresses on the node's networks (the same rule as remote downloads) are refused unless they are in `remote_download_allowlist`;
  `disable_remote_download` turns the importer off. Cancelling the request cancels the import.

## Build

`go build` needs Linux system calls, so build for Linux: `GOOS=linux GOARCH=amd64 go build -o wings .`. The tests of the new package run
anywhere: `go test ./internal/certstore`. The rest of the tests of Wings need Linux, as before.

## Releases

The `nightly` release is built by `.github/workflows/nightly.yaml` on every push to `develop` and every night: it runs the tests, builds Linux
binaries (amd64, arm64) with checksums, and moves the `nightly` tag to the commit it built.
