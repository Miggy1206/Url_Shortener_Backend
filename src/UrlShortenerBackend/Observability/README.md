# Observability

![alt text](observabilityDashboard.png)

The URL Shortener Backend uses OpenTelemetry-based observability to monitor application behaviour, Kafka event publishing and processing, resilience mechanisms, and service performance during local development.

The observability stack provides three complementary capabilities:

- **Metrics** for measuring application behaviour, resilience, and performance.
- **Traces** for following requests and asynchronous event processing across components.
- **Dashboards** for visualising operational data.

---

## Architecture

The current local observability architecture is:

```text
                         URL Shortener API
                                |
                     +----------+----------+
                     |                     |
                  Metrics                Traces
                     |                     |
                 /metrics                  |
                     |              OpenTelemetry
                     v                 Collector
                Prometheus                |
                     |               +----+----+
                     v               |         |
                  Grafana          Jaeger    Future exporters
```

The application is instrumented using OpenTelemetry rather than being coupled directly to a particular monitoring vendor.

Metrics are exposed through the Prometheus exporter, while tracing is sent using OTLP to the OpenTelemetry Collector.

---

## Components

| Component               | Purpose                                    |
| ----------------------- | ------------------------------------------ |
| OpenTelemetry SDK       | Collects application metrics and traces    |
| OpenTelemetry Collector | Receives and forwards trace telemetry      |
| Prometheus              | Scrapes and stores application metrics     |
| Grafana                 | Visualises metrics and provides dashboards |
| Jaeger                  | Stores and visualises distributed traces   |

---

## Local Endpoints

The local development stack exposes:

| Service    | URL                                                            | Purpose                     |
| ---------- | -------------------------------------------------------------- | --------------------------- |
| API        | [http://localhost:8080](http://localhost:8080)                 | Application                 |
| Metrics    | [http://localhost:8080/metrics](http://localhost:8080/metrics) | Prometheus metrics endpoint |
| Prometheus | [http://localhost:9090](http://localhost:9090)                 | Metrics querying            |
| Grafana    | [http://localhost:3100](http://localhost:3100)                 | Dashboards                  |
| Jaeger     | [http://localhost:16686](http://localhost:16686)               | Distributed tracing         |

The OpenTelemetry Collector communicates internally with the other Docker services and does not need its OTLP ports exposed to the host for the Dockerised application.

---

## Metrics

The API exposes Prometheus-compatible metrics from the `/metrics` endpoint.

### Built-in Metrics

OpenTelemetry instrumentation collects application and runtime telemetry including:

- ASP.NET Core HTTP request metrics
- .NET runtime metrics
- .NET process metrics
- Redis instrumentation metrics

---

## Custom Application Metrics

The application exposes custom metrics for Kafka-based click processing and Kafka resilience.

### Click Events Published

```text
urlshortener_click_events_published_total
```

Counts successfully published click events.

Example query:

```promql
urlshortener_click_events_published_total
```

Events published per second:

```promql
rate(urlshortener_click_events_published_total[5m])
```

---

### Click Events Processed

```text
urlshortener_click_events_processed_total
```

Counts click events successfully processed by the Kafka consumer and persisted to PostgreSQL.

Example query:

```promql
urlshortener_click_events_processed_total
```

Events processed per second:

```promql
rate(urlshortener_click_events_processed_total[5m])
```

---

### Duplicate Click Events

```text
urlshortener_click_events_duplicates_total
```

Counts duplicate events skipped by the idempotency mechanism.

Duplicate events do not increment the click count again.

---

### Kafka Publish Failures

```text
urlshortener_click_events_publish_failures_total
```

Counts click events that failed to publish to Kafka after the configured retry attempts were exhausted.

These failures do not prevent the redirect from being returned to the client.

---

### Kafka Publish Retries

```text
urlshortener_click_events_publish_retries_total
```

Counts retry attempts made when Kafka publishing initially fails.

Example query:

```promql
rate(urlshortener_click_events_publish_retries_total[5m])
```

A normally healthy system should have a very low retry rate. A sustained increase can indicate Kafka connectivity or availability problems.

---

### Kafka Publish Duration

```text
urlshortener_kafka_publish_duration_milliseconds
```

A histogram measuring the duration of Kafka publish attempts.

Average publish latency:

```promql
rate(urlshortener_kafka_publish_duration_milliseconds_sum[5m])
/
rate(urlshortener_kafka_publish_duration_milliseconds_count[5m])
```

p95 publish latency:

```promql
histogram_quantile(
  0.95,
  sum by (le) (
    rate(urlshortener_kafka_publish_duration_milliseconds_bucket[5m])
  )
)
```

Publishing latency can increase significantly when Kafka is unavailable because the producer waits for the configured Kafka timeout before the attempt fails.

---

### Click Event Processing Duration

```text
urlshortener_click_events_processing_duration_milliseconds
```

A histogram measuring the duration of click-event processing, including the PostgreSQL transaction and click-count update.

p95 processing latency:

```promql
histogram_quantile(
  0.95,
  sum by (le) (
    rate(urlshortener_click_events_processing_duration_milliseconds_bucket[5m])
  )
)
```

---

## Kafka Circuit Breaker Metrics

The Kafka producer uses a Polly circuit breaker to prevent the application from repeatedly attempting to communicate with an unavailable Kafka broker.

The circuit has three operational states:

```text
CLOSED
   |
   | repeated failures
   v
OPEN
   |
   | break duration expires
   v
HALF-OPEN
   |
   | successful trial
   v
CLOSED
```

### Circuit Opened

```text
urlshortener_kafka_circuit_opened_total
```

Counts the number of times the Kafka circuit breaker has transitioned to the open state.

Example query:

```promql
urlshortener_kafka_circuit_opened_total
```

---

### Circuit Half-Opened

```text
urlshortener_kafka_circuit_half_opened_total
```

Counts the number of times the circuit breaker has transitioned from open to half-open and allowed a recovery attempt.

Example query:

```promql
urlshortener_kafka_circuit_half_opened_total
```

---

### Circuit Closed

```text
urlshortener_kafka_circuit_closed_total
```

Counts the number of times the circuit breaker has transitioned back to the closed state after successful recovery.

Example query:

```promql
urlshortener_kafka_circuit_closed_total
```

These are cumulative event counters and should not be interpreted as the current circuit state.

---

## Kafka Resilience

Kafka publishing currently uses two resilience mechanisms:

### Retry

Transient Kafka failures are retried using a short exponential backoff.

The current retry sequence is:

```text
Attempt 1
   |
   | failure
   v
50 ms delay
   |
   v
Attempt 2
   |
   | failure
   v
100 ms delay
   |
   v
Attempt 3
   |
   | failure
   v
Publish failure
```

The retry configuration is application configuration driven and can be adjusted without changing the core publishing logic.

### Circuit Breaker

The circuit breaker protects the application when Kafka remains unavailable.

After repeated failures, the circuit opens and subsequent Kafka publish attempts are rejected immediately until the break duration expires.

This prevents every redirect request from repeatedly waiting for Kafka to time out during a prolonged outage.

The redirect path deliberately treats Kafka click tracking as non-critical:

```text
Client
  |
  v
Redirect request
  |
  +---- Redis / PostgreSQL lookup
  |
  +---- Kafka click event
          |
          +---- success -> continue
          |
          +---- failure -> log and continue
  |
  v
302 Redirect
```

This means the URL redirect remains available even when Kafka is unavailable.

---

## Grafana Dashboard

Grafana is used to visualise metrics collected by Prometheus.

The current dashboard focuses on the application's Kafka click-processing pipeline and resilience behaviour.

### Headline Panels

Recommended headline panels:

- Kafka Click Events Published
- Click Events Processed
- Kafka Publish Failures
- Kafka Publish Retries
- Duplicate Click Events
- Unprocessed Click Events
- Kafka Circuit Opened
- Kafka Circuit Half-Opened
- Kafka Circuit Closed

### Time-Series Panels

Recommended time-series panels:

- Kafka Publish Rate
- Kafka Retry Rate
- Kafka Failure Rate
- Kafka Publish Latency (p95)
- Kafka Publish Latency (Average)
- Click Event Processing Latency (p95)
- Click Event Processing Latency (Average)
- Published vs Processed Click Events
- Kafka Circuit State Transitions

A useful dashboard layout is:

```text
+----------------------+----------------------+----------------------+
| Events Published     | Events Processed     | Publish Failures    |
+----------------------+----------------------+----------------------+
| Publish Retries      | Duplicate Events     | Unprocessed Events  |
+----------------------+----------------------+----------------------+
| Circuit Opened       | Circuit Half-Opened  | Circuit Closed      |
+----------------------+----------------------+----------------------+
| Kafka Publish Rate   | Kafka Retry Rate     | Kafka Failure Rate  |
+----------------------+----------------------+----------------------+
| Kafka Publish p95    | Processing p95       |                     |
+----------------------+----------------------+----------------------+
| Published vs Processed Click Events                             |
+-----------------------------------------------------------------+
| Kafka Circuit State Transitions                                 |
+-----------------------------------------------------------------+
```

The dashboard should normally be viewed over the **last 15 minutes** during local development and configured to refresh every few seconds when actively testing the system.

---

## Tracing

The application uses `ActivitySource` to create application-level spans.

The current tracing model includes:

```text
HTTP request
    |
    +-- Redis operation
    |
    +-- kafka.publish
            |
            v
        Kafka topic
            |
            v
    click_event.process
            |
            v
       PostgreSQL
```

Kafka trace context is propagated through Kafka message headers so that the consumer can continue the trace created by the producer.

This allows asynchronous processing to be correlated with the original HTTP request.

---

## Trace Attributes

Kafka-related spans include useful attributes such as:

```text
messaging.system
messaging.destination.name
messaging.kafka.partition
messaging.kafka.offset
urlshortener.event_id
urlshortener.short_code
```

These attributes make it possible to identify which Kafka event, short code, partition, and offset are associated with a trace.

---

## OpenTelemetry Collector

The collector receives OTLP trace data from the application and forwards it to Jaeger.

The configuration is located at:

```text
src/UrlShortenerBackend/Observability/otel-collector-config.yaml
```

The collector configuration currently contains:

```text
OTLP receiver
    |
    v
batch processor
    |
    v
Jaeger exporter
```

The Dockerised API communicates with the collector using the Docker Compose service name:

```text
http://otel-collector:4317
```

The application can use a local OTLP endpoint when running outside Docker.

---

## Prometheus Configuration

The Prometheus configuration is located at:

```text
src/UrlShortenerBackend/Observability/prometheus.yml
```

Prometheus scrapes the Dockerised API using the Docker Compose service name:

```text
api:8080/metrics
```

The current scrape interval is:

```text
5 seconds
```

This provides sufficiently frequent samples for local dashboard development and rate-based queries.

---

## Running the Observability Stack

From the repository root:

```bash
docker compose --env-file .env.local up -d
```

Check that the observability containers are running:

```bash
docker ps
```

The expected observability containers are:

```text
urlshortener-otel-collector
urlshortener-jaeger
urlshortener-prometheus
urlshortener-grafana
```

---

## Verifying Metrics

Check the API metrics endpoint:

```bash
curl http://localhost:8080/metrics
```

To inspect custom application metrics:

```bash
curl -s http://localhost:8080/metrics | grep "^# TYPE urlshortener"
```

To inspect Kafka metrics specifically:

```bash
curl -s http://localhost:8080/metrics | grep "kafka"
```

Check Prometheus:

```text
http://localhost:9090
```

The API target should be shown as healthy.

Grafana is available at:

```text
http://localhost:3100
```

Jaeger is available at:

```text
http://localhost:16686
```

---

## Generating Telemetry

Generate a redirect using an existing short code:

```bash
curl -i http://localhost:8080/YOUR_SHORT_CODE
```

A successful request should result in:

```text
HTTP/1.1 302 Found
```

The normal processing path is:

```text
HTTP request
    |
    +-- URL lookup
    |
    +-- Redis cache
    |
    +-- Kafka click event published
             |
             v
        Kafka topic
             |
             v
        Consumer
             |
             v
    Click event processor
             |
             v
        PostgreSQL
             |
             v
       Click count +1
```

The corresponding metrics and traces should then become visible in Prometheus, Grafana, and Jaeger.

---

## Testing Observability

Observability should be tested alongside the application rather than treated as an independent feature.

### Normal Click Processing

```text
Redirect
  -> Kafka publish
  -> Consumer processing
  -> PostgreSQL click-count update
```

Expected results:

- Published events increase.
- Processed events increase.
- Publish failures remain unchanged.
- Retry count remains unchanged when Kafka is healthy.
- Processing latency is recorded.
- A corresponding trace can be inspected in Jaeger.

---

### Duplicate Event

Publishing the same event twice should result in:

```text
Published events  +2
Processed events  +1
Duplicate events  +1
```

The click count should only increase once because of the idempotency mechanism.

---

### Kafka Failure

Kafka availability can be tested by stopping the broker:

```bash
docker stop urlshortener-kafka
```

Repeated redirect requests should still return successfully because Kafka click tracking is non-critical to the redirect response.

During the outage, the observability stack should show:

```text
Kafka Publish Retries       increases
Kafka Publish Failures      increases
Kafka Publish Latency       increases
Kafka Circuit Opened        increases
```

After the circuit opens, Kafka publishing attempts should be rejected by the circuit breaker instead of repeatedly contacting the unavailable broker.

The application continues to return the redirect while recording the Kafka failure in logs and metrics.

---

### Kafka Recovery

Restart Kafka:

```bash
docker start urlshortener-kafka
```

After the circuit break duration expires, the circuit enters half-open and allows a recovery attempt.

A successful recovery should result in:

```text
Circuit Opened
      |
      v
Circuit Half-Opened
      |
      v
Circuit Closed
```

Kafka publishing should then resume normally.

---

## Observed Resilience Behaviour

The Kafka architecture was benchmarked before and after asynchronous click-event persistence.

### Before Kafka Asynchronous Processing

```text
Requests:       ~207 req/s
Average:        ~120.50 ms
p95:            ~330.09 ms
Errors:         0%
```

### After Kafka Asynchronous Processing

```text
Requests:       ~2,173 req/s
Average:        ~11.38 ms
p95:            ~14.06 ms
Errors:         0%
```

This reduced the synchronous PostgreSQL work on the redirect path and substantially improved throughput and latency.

The resilience work then added Kafka retries and a circuit breaker so that dependency failures are handled without making Kafka a single point of failure for URL redirection.

---

## Future Improvements

The current observability implementation provides a strong local monitoring foundation.

Potential future improvements include:

- PostgreSQL tracing instrumentation
- Liveness and readiness endpoints
- Additional business-level metrics
- Kafka consumer lag monitoring
- Alerting
- Grafana dashboard provisioning
- More detailed HTTP latency dashboards
- Improved trace correlation and propagation
- Kafka consumer health metrics
- Circuit-breaker state gauges
- AWS monitoring integration
- Cloud-based telemetry

The largest remaining resilience improvement is a **transactional outbox**.

The current application protects redirect availability when Kafka is unavailable, but a click event can still be lost if it cannot be published.

A transactional outbox could address this by persisting the click event to PostgreSQL before publishing it asynchronously:

```text
Redirect
   |
   v
PostgreSQL transaction
   |
   +-- click update / outbox event
   |
   v
Outbox publisher
   |
   v
Kafka
```

This would provide durable event delivery while retaining the current asynchronous architecture.

The same OpenTelemetry instrumentation can be retained when the application moves to AWS, allowing the observability backend to evolve without tightly coupling application code to Grafana, Jaeger, or another specific vendor.

---

## Directory Structure

```text
src/UrlShortenerBackend/Observability/

├── README.md
├── UrlShortenerActivitySource.cs
├── UrlShortenerMetrics.cs
├── otel-collector-config.yaml
└── prometheus.yml
```
