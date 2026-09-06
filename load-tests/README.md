# Load Tests

Load testing for the URL Shortener Backend is performed using [k6](https://k6.io/).

These tests are intended to measure application performance under concurrent load and identify potential bottlenecks.

## Prerequisites

- k6 installed
- PostgreSQL running
- Redis running
- API running locally
- A valid shortened URL available for the redirect tests

## Test Scripts

| Script             | Purpose                                                        |
| ------------------ | -------------------------------------------------------------- |
| `redirect.js`      | Measures the complete redirect request path                    |
| `database-read.js` | Measures PostgreSQL URL reads in isolation                     |
| `click-update.js`  | Measures the atomic PostgreSQL click-count update in isolation |

## Running the Tests

### Redirect Performance

```bash
k6 run load-tests/redirect.js
```

This test sends concurrent requests to the real:

```text
GET /{shortCode}
```

redirect endpoint.

The benchmark uses 25 virtual users for 30 seconds.

Rate limiting is temporarily disabled when running the raw performance benchmark so that requests are not rejected by the application's `60 requests/minute` redirect limit.

The rate limiter should be restored after benchmarking.

### PostgreSQL Read

```bash
k6 run load-tests/database-read.js
```

This measures the performance of retrieving a URL from PostgreSQL without performing a click-count update.

It provides a comparison point for determining whether database reads contribute significantly to redirect latency.

### Click-Count Update

```bash
k6 run load-tests/click-update.js
```

This measures the atomic PostgreSQL operation used to increment the click count:

```text
ClickCount = ClickCount + 1
```

It isolates the database write from the rest of the redirect request.

## Results

Initial local benchmarks were performed using:

```text
25 virtual users
30 second duration
```

| Test                |   Throughput | Average Latency | p95 Latency | Error Rate |
| ------------------- | -----------: | --------------: | ----------: | ---------: |
| PostgreSQL read     | ~3,020 req/s |         8.06 ms |    14.61 ms |         0% |
| Atomic click update |   ~204 req/s |       122.11 ms |   327.78 ms |         0% |
| Full redirect       |   ~207 req/s |       120.50 ms |   330.09 ms |         0% |

## Findings

The PostgreSQL read-only workload significantly outperformed the other tests.

The atomic click-count update produced almost identical performance to the complete redirect request.

This indicates that the synchronous click-count database update is currently the primary performance bottleneck when multiple requests attempt to update the same URL concurrently.

The current implementation prioritises correctness by using an atomic database update to prevent lost click-count updates.

Future performance work will investigate decoupling click-count persistence from the redirect request path.

## Rate Limiting

The application's normal redirect rate limit is:

```text
60 requests per minute per client
```

Load tests intended to measure raw application capacity must therefore be run with the rate limiter temporarily disabled.

Rate limiting should remain enabled for normal development and production operation.

## Interpreting k6 Results

The most useful metrics when comparing runs are:

- **Requests per second** — overall throughput
- **Average latency** — typical request duration
- **p95 latency** — latency experienced by the slowest 5% of requests
- **Error rate** — percentage of failed requests

Performance changes should be compared against the baseline above rather than judged from a single metric.
