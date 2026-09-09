# Load Tests

Load testing for the URL Shortener Backend is performed using [k6](https://k6.io/).

These tests are intended to measure application performance under concurrent load, identify bottlenecks, and validate the impact of performance improvements.

## Prerequisites

- k6 installed
- PostgreSQL running
- Redis running
- Kafka running
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

## Baseline Results

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

This indicated that the synchronous click-count database update was the primary performance bottleneck when multiple requests attempted to update the same URL concurrently.

The synchronous implementation prioritised correctness by using an atomic database update to prevent lost click-count updates, but this placed a database write directly on the redirect request path.

## Kafka-Based Asynchronous Click Processing

Click-count persistence was subsequently decoupled from the redirect request using Kafka.

The request path is now:

```text
GET /{shortCode}
       │
       ├── Redis lookup
       │
       ├── Publish UrlClickedEvent to Kafka
       │
       └── Return 302
                  │
                  ▼
             Kafka topic
                  │
                  ▼
        ClickEventConsumer
                  │
                  ▼
             PostgreSQL
```

This removes the synchronous PostgreSQL click-count update from the redirect path while retaining asynchronous persistence and idempotent event processing.

## Post-Kafka Results

A second local benchmark was performed using the same:

```text
25 virtual users
30 second duration
```

| Test                         |   Throughput | Average Latency | p95 Latency | Error Rate |
| ---------------------------- | -----------: | --------------: | ----------: | ---------: |
| Full redirect (before Kafka) |   ~207 req/s |       120.50 ms |   330.09 ms |         0% |
| Full redirect (after Kafka)  | ~2,173 req/s |        11.38 ms |    14.06 ms |         0% |

The post-Kafka benchmark resulted in approximately:

- **10.5× higher throughput**
- **10.6× lower average latency**
- **23.5× lower p95 latency**
- **0% errors in both runs**

These results demonstrate that removing the synchronous PostgreSQL click-count update from the redirect request path substantially improves performance under concurrent load.

The benchmark measures the complete application path, including Kafka event publication, so the improvement should be interpreted as the result of the architectural change as a whole rather than attributed to Kafka alone.

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

Performance changes should be compared against the recorded baseline using consistent virtual-user counts and test durations.
