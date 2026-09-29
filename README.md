# YourPortal Stress Test

A single-file, dependency-free Python tool for load testing a GPS-telemetry ingestion server. It simulates a fleet of popular GPS trackers speaking the text protocol over **TCP** and **UDP**, and it also load tests a **REST** telemetry endpoint. It measures connection capacity, throughput and latency percentiles, breaks errors down by type, can check how many rows actually landed in **ClickHouse**, and writes a Markdown + JSON report for each run.

```
 trackers (simulated)          your server                     storage
 ┌───────────────────┐   TCP :9001   ┌──────────────┐
 │ N × Device msg    │ ────────────▶ │   gateway    │ ──▶ Kafka ──▶ ClickHouse
 │ N × Device msg    │   UDP :9001   │              │                   ▲
 │ REST JSON clients │ ── HTTP ────▶ │ REST :8090   │                   │
 └───────────────────┘               └──────────────┘   row count before/after
                                                          (local HTTP or via SSH)
```

## Features

- **TCP fleet test:** N persistent connections, spread over a ramp-up window. Each simulated device sends K packets at a set interval and then disconnects.
- **UDP burst test:** sends `devices × rounds` datagrams as fast as the socket allows. It is capped at 10,000 simulated devices.
- **REST test:** `POST`s JSON telemetry through a pool of concurrent workers. It is capped at 500 requests per run.
- **Latency stats:** min / p50 / p95 / p99 / max for TCP connect, TCP send and the REST round trip.
- **Baseline ICMP ping** to the target, so you can compare against network RTT.
- **End-to-end verification:** counts rows in the ClickHouse table before and after each test, to show what was actually stored rather than only what was sent.
- **Error accounting:** errors are grouped by signature (`TCP Connect: TimeoutError`, `HTTP Status: 502 …`), each with a probable root cause.
- **Reports:** a colour-coded summary table in the terminal, plus `reports/stress_test_report_<timestamp>.md` and `.json`.
- **No third-party packages:** it uses only the standard library (`asyncio`, `socket`, `urllib`).

## Requirements

- Python 3.8+
- Linux or macOS. The script uses the `resource` module to raise the open-file limit, so it does not run on Windows. WSL works.
- The `ping` command. It is optional and is only used for the baseline RTT.
- For ClickHouse verification against a remote server: passwordless SSH access (key-based, `BatchMode=yes`) to that server, and `curl` installed on it.

## Quick start

```bash
git clone https://github.com/<you>/<repo>.git
cd <repo>

# Smoke test: 50 devices, TCP only, no ClickHouse check
python3 stress-test.py --host your-server.example.com --mode tcp --devices 50 --no-ch
```

## Usage

```
python3 stress-test.py [options]
```

| Option | Default | Description |
|---|---|---|
| `--mode {tcp,udp,rest,all}` | `all` | Which test suite(s) to run |
| `--host HOST` | from `CONFIG` | Target server hostname or IP |
| `--tcp-port N` | `9001` | Device TCP port |
| `--udp-port N` | `9001` | Device UDP port |
| `--rest-port N` | `8090` | REST port (for example 8090 behind Nginx, 3002 for a direct backend) |
| `--rest-path PATH` | `/api/telemetry` | REST endpoint path |
| `--devices N` | `1000` | Number of simulated devices / TCP connections |
| `--rounds N` | `2` | Packets per device |
| `--interval SEC` | `1.0` | Delay between one device's packets (`0` = burst) |
| `--ramp SEC` | `3.0` | Time window for opening all TCP connections |
| `--concurrency N` | `30` | Number of parallel REST workers |
| `--report-dir DIR` | `./reports` | Where reports are saved |
| `--no-report` | — | Don't write report files |
| `--no-ch` | — | Skip the ClickHouse row-count check |
| `--verbose` | — | Print the first few errors of each type |

The rest of the settings are in the `CONFIG` block at the top of the script: ClickHouse host, port, database and table, the SSH host, `REST_USE_HTTPS`, the socket timeout and the progress interval. Command-line flags override `CONFIG`.

## Example

Suppose you run this test from your laptop against a server in the cloud. The goal is 2,000 trackers, each sending 5 packets 2 seconds apart, with a 10-second ramp-up. The test should also confirm that every packet reached ClickHouse.

1. Edit `CONFIG` once:
   ```python
   "TARGET_HOST": "your-server.example.com",
   "CLICKHOUSE_SSH_HOST": "root@your-server.example.com",
   "CLICKHOUSE_DB": "yourportal_telemetry",
   "CLICKHOUSE_TABLE": "telemetry",
   ```
2. Check that SSH works without a password prompt:
   ```bash
   ssh -o BatchMode=yes root@your-server.example.com "echo ok"
   ```
3. Run the test:
   ```bash
   python3 stress-test.py --mode tcp --devices 2000 --rounds 5 --interval 2 --ramp 10
   ```

The run takes about 10 s of ramp-up, plus 4 × 2 s of sending, plus 5 s of waiting for ClickHouse to flush. It prints live progress:

```
  [  4.0s] Connected: 812/2,000 | Active Conns: 812 | Sent: 1,204 | Errors: 0
  ...
--- TCP Test Results ---
  Connected:          2,000 / 2,000 (100.0%)
  Packets Sent:       10,000 / 10,000 (100.0%)
  Connection Latency: p50=41.3ms, p95=88.0ms, p99=140.2ms
  ClickHouse telemetry rows: before=1,532,004, after=1,542,004, added=10,000
```

Here `added` equals `devices × rounds`, so nothing was lost anywhere on the path from gateway to Kafka to ClickHouse.

### More examples

```bash
# Run all three suites on the server itself (ClickHouse is queried directly, no SSH)
python3 stress-test.py --host 127.0.0.1 --rest-port 3002

# UDP flood: 10,000 devices × 3 packets, sent as fast as possible
python3 stress-test.py --mode udp --devices 10000 --rounds 3

# REST through Nginx, 50 parallel workers, with errors printed
python3 stress-test.py --mode rest --devices 500 --concurrency 50 --verbose

# Find the connection limit: step up the fleet size
for n in 1000 5000 10000 20000; do
  python3 stress-test.py --mode tcp --devices $n --rounds 1 --ramp 20 --no-ch
done
```

## Reading the results

Each suite gets one of three statuses:

| Status | TCP | UDP | REST |
|---|---|---|---|
| **PASS** | ≥ 99% connected **and** ≥ 99% packets sent | ≥ 99% sent **and** ClickHouse rows increased (if the check is enabled) | ≥ 99% HTTP 200/201 |
| **DEGRADED** | ≥ 90% connected | ≥ 90% sent | ≥ 90% OK |
| **FAIL** | below that | below that | below that |

The overall verdict is PASS only if every suite that ran passed. It is FAIL if any suite failed, and DEGRADED otherwise.

**Important caveats:**

- **UDP "sent" means the packet left the local socket.** It does not mean the server received it, because UDP has no delivery confirmation. The real measure of UDP loss is the **ClickHouse `added`** figure compared with the number sent.
- **TCP packet latency** is the time for `writer.drain()`, which is how long it took to hand the data to the OS send buffer. It is not a server-side acknowledgement. Device packets (e.g. Megastek) get no application-level reply here. Connection latency (the TCP handshake) is the more meaningful figure.
- **ClickHouse `added` counts every row inserted during the test window.** If real devices are writing to the same table at the same time, the number will be higher than the number of packets you sent. Use a staging environment, or check which IMEIs were written: test devices use the prefixes `861…` (TCP), `860…` (UDP) and `862…` (REST).
- The script waits a fixed **5 s** for Kafka and ClickHouse to flush. If your pipeline batches more slowly, `added` will be too low.
- The text under "WAN Insights" in the Markdown report is a fixed template. It is not generated from measurements, so edit it in `generate_reports()` to describe your own architecture.

## Tips for large runs

- The script raises its own `nofile` limit, but it can't go above the system hard limit. For more than about 10,000 connections, check `ulimit -Hn` on the client and on the server.
- From a single client IP to a single port there are roughly 28,000 ephemeral ports. For bigger fleets, widen `net.ipv4.ip_local_port_range` or run the test from several machines.
- A long `--ramp` avoids overflowing the SYN backlog. If you see `TimeoutError` or `ConnectionReset` during connect, try a longer ramp before you blame the server.
- Run once on `127.0.0.1` and once over the WAN. The difference tells you how much of the result comes from the network rather than the server.

## Packet format

Each simulated tracker sends a Megastek `$MG` sentence with the current date and time and a fixed position (near 49.7094 N, 81.6088 E), which shifts slightly on every round:

```
$MG,861000000000042,000,R,300926,011500,A,4942.5640,N,08136.5280,E,000,000,14,000,15.00,...,10,95,;
```

The REST payload looks like this:

```json
{"deviceId":"862000000000042","lat":49.7094,"lon":81.6088,"speed":15.0,
 "battery":95,"satellites":12,"location_type":"GNSS","accuracy":5.0}
```

## Security note

Only point this tool at servers you own or are authorised to test. Even at the default settings it opens thousands of connections, which can look like a DoS attack to a hosting provider.

## License

MIT (or your license of choice).
