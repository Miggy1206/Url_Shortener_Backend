# URL Shortener Backend

A distributed URL shortening service built with **C# and .NET 10**.

The project is a practical exploration of modern backend and distributed systems engineering, progressing from a simple REST API into a scalable, observable, resilient, production-oriented service.

The focus is on understanding how distributed services are designed, tested, containerised, deployed, monitored, and scaled, while exploring technologies such as PostgreSQL, Redis, Kafka, Docker, AWS, and OpenTelemetry.

---

## Table of Contents

- [Objectives](#objectives)
- [Architecture](#architecture)
- [Transactional Outbox](#transactional-outbox)
- [Setup](#setup)
- [API](#api)
- [Docker](#docker)
- [CI/CD](#cicd)
- [Tech Stack](#tech-stack)
- [Testing](#testing)
- [Observability](#observability)
- [Load Testing](#load-testing)
- [Project Status](#project-status)

---

## Objectives

The main objectives of this project are to:

- Build a robust REST API using **ASP.NET Core and .NET 10**.
- Explore **distributed systems architecture**, scalability, availability, resilience, and fault tolerance.
- Develop practical experience with **PostgreSQL and Entity Framework Core**.
- Use **Redis** for distributed caching and performance optimisation.
- Use **Apache Kafka** for asynchronous event processing and event-driven architecture.
- Implement the **transactional outbox pattern** for durable event delivery.
- Apply automated **unit and integration testing** throughout development.
- Learn containerisation and service orchestration using **Docker and Kubernetes**.
- Explore **AWS** and cloud-based infrastructure.
- Implement **observability** using metrics, tracing, dashboards, and structured logging.
- Understand concepts such as **load balancing, service communication, caching, concurrency, messaging, observability, resilience, retries, circuit breakers, fault isolation, and durable event delivery**.
- Apply software engineering principles around **architecture, maintainability, scalability, security, and performance**.

---

## Architecture

The application currently consists of an ASP.NET Core API backed by PostgreSQL and Redis, with Kafka used to asynchronously process click events.

A simplified redirect flow is:

```text
Client
   |
   v
ASP.NET Core API
   |
   v
Redis cache
   |
   +-- Cache hit --------------------------+
   |                                       |
   +-- Cache miss -> PostgreSQL            |
                                           |
                                           v
                              Original destination
                                           |
                                           v
                              PostgreSQL transaction
                                           |
                                           v
                                   OutboxMessage
                                           |
                                           v
                                   302 Redirect
                                           |
                                           v
                                  OutboxPublisher
                                           |
                                  +--------+--------+
                                  |                 |
                               Retry            Circuit
                                  |              Breaker
                                  +--------+--------+
                                           |
                                           v
                                         Kafka
                                           |
                                           v
                                  ClickEventConsumer
                                           |
                                           v
                                  ClickEventProcessor
                                           |
                                           v
                                      PostgreSQL
                                   (ClickCount + 1)
```

The redirect path no longer performs the click-count database update synchronously.

Instead, a valid redirect creates a durable `OutboxMessage` inside the same PostgreSQL transaction used by the application. The redirect can then complete without waiting for Kafka or the click-count update.

The outbox publisher asynchronously reads unpublished events and publishes them to Kafka.

Kafka publishing is protected by retry and circuit-breaker mechanisms so that Kafka outages do not prevent the application from serving redirects.

---

## Transactional Outbox

The project implements the **transactional outbox pattern** to improve event delivery reliability.

The core problem with publishing directly to Kafka from the redirect request is that the application could successfully serve the redirect while the Kafka publish fails.

The transactional outbox changes the sequence to:

```text
Redirect request
      |
      v
PostgreSQL transaction
      |
      +-- Application data
      |
      +-- OutboxMessage
      |
      v
Transaction committed
      |
      v
302 Redirect
      |
      v
OutboxPublisher
      |
      v
Kafka
```

The important guarantee is that once the PostgreSQL transaction commits, the click event is durably stored even if Kafka is unavailable.

### Outbox Message

Each outbox record contains:

```text
Id
Type
Payload
OccurredAt
PublishedAt
AttemptCount
LastAttemptAt
LastError
ProcessingStartedAt
```

The event currently stored in the outbox is:

```text
UrlClickedEvent
    |
    +-- EventId
    +-- ShortCode
    +-- OccurredAt
```

### Concurrent Outbox Publishers

Multiple publisher instances can safely operate against the same outbox.

Messages are claimed using PostgreSQL row-level locking with:

```sql
FOR UPDATE SKIP LOCKED
```

This prevents multiple publisher instances from simultaneously claiming the same message.

The claim process is:

```text
Publisher A                  Publisher B
     |                            |
     v                            v
SELECT ... FOR UPDATE       SELECT ... FOR UPDATE
SKIP LOCKED                  SKIP LOCKED
     |                            |
     v                            v
Claim message               Skip locked message
```

This allows publishers to process different messages concurrently without requiring a distributed lock.

### Failed Publishing

If Kafka publication fails:

```text
Publish attempt fails
        |
        v
ProcessingStartedAt = NULL
        |
        v
LastError recorded
        |
        v
Message remains unpublished
        |
        v
Retry during a future polling cycle
```

This ensures failed messages remain durable and available for retry.

### Stale Claims

A publisher can crash after claiming a message but before completing publication.

To recover from this situation, claims have a timeout.

Messages with an expired `ProcessingStartedAt` can be reclaimed by another publisher.

This prevents abandoned claims from permanently blocking event delivery.

### Kafka Outage Recovery

The outbox has been tested against an actual local Kafka outage.

During testing:

```text
Kafka stopped
     |
     v
Redirects continue returning 302
     |
     v
Click events accumulate in PostgreSQL
     |
     v
Kafka restarted
     |
     v
OutboxPublisher drains backlog
     |
     v
Kafka consumer processes events
     |
     v
Click counts catch up
```

This demonstrates that temporary Kafka unavailability no longer causes click events to be lost.

---

## Kafka Delivery Model

The redirect request **does not wait for Kafka**.

Instead, it waits only for the PostgreSQL transaction containing the outbox event to commit.

This provides a durable acknowledgement point without putting Kafka latency directly on the redirect path.

The delivery path is therefore:

```text
HTTP request
    |
    v
PostgreSQL transaction
    |
    +-- OutboxMessage persisted
    |
    v
302 response
```

and separately:

```text
OutboxMessage
    |
    v
OutboxPublisher
    |
    v
Kafka
    |
    v
ClickEventConsumer
```

### Retry and Backoff

Transient Kafka publishing failures are retried using a short exponential backoff.

The resilience layer prevents temporary Kafka failures from immediately becoming permanent event-delivery failures.

### Circuit Breaker

Repeated Kafka failures cause the Kafka publisher circuit breaker to open.

When open:

```text
OutboxPublisher
       |
       v
Kafka circuit OPEN
       |
       +---- Kafka publish rejected/fails fast
       |
       v
Outbox message remains unpublished
```

Once the configured break duration expires, the circuit enters half-open and allows a trial operation.

A successful operation closes the circuit and normal publishing resumes.

The transactional outbox means the circuit breaker no longer determines whether the click event is durable: the event already exists in PostgreSQL.

---

## Setup

### Prerequisites

- .NET 10 SDK
- Docker Desktop
- Git

### Clone the Repository

```bash
git clone <repository-url>

cd Url_Shortener_Backend
```

### Configure Environment Variables

Create a local `.env.local` file for Docker Compose:

```env
POSTGRES_DB=urlshortener
POSTGRES_USER=postgres
POSTGRES_PASSWORD=<your-password>
```

This file should remain local and must not be committed to source control.

### Run the Infrastructure

Start PostgreSQL, Redis, Kafka, the OpenTelemetry Collector, Jaeger, Prometheus, and Grafana using Docker Compose:

```bash
docker compose --env-file .env.local up -d
```

The local infrastructure exposes:

```text
PostgreSQL -> localhost:5433
Redis      -> localhost:6379
Kafka      -> localhost:9093
Prometheus -> localhost:9090
Grafana    -> localhost:3100
Jaeger     -> localhost:16686
```

Inside the Docker Compose network, application containers communicate with Kafka using:

```text
kafka:9092
```

The API sends OpenTelemetry trace data to the collector using:

```text
http://otel-collector:4317
```

### Configure the API for Local Development

For running the API directly from the host, configure the PostgreSQL connection using .NET User Secrets:

```bash
cd src/UrlShortenerBackend

dotnet user-secrets init

dotnet user-secrets set \
  "ConnectionStrings:DefaultConnection" \
  "Host=localhost;Port=5433;Database=urlshortener;Username=postgres;Password=<your-password>"
```

The local API connects to Redis using:

```text
localhost:6379
```

For local Kafka development:

```text
localhost:9093
```

### Apply Database Migrations

```bash
dotnet ef database update
```

### Create the Kafka Topic

The production/local Docker topic is:

```text
url-clicked
```

Create it using:

```bash
docker exec urlshortener-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic url-clicked \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

The topic only needs to be created once unless the Kafka data volume is recreated.

### Run the API Locally

```bash
dotnet run
```

Swagger/OpenAPI can then be used to interact with the API.

### Run Tests

From the repository root:

```bash
dotnet test
```

The test suite includes unit tests, database integration tests, concurrency tests, Kafka integration tests, resilience tests, outbox tests, and circuit-breaker tests.

---

## API

### Create Short URL

```http
POST /api/urls
```

Creates a shortened URL from a provided original URL.

Example request:

```json
{
  "originalUrl": "https://www.example.com"
}
```

A successful request returns:

```text
201 Created
```

The response contains the generated short code, short URL, and original URL.

### URL Validation

The API validates URLs before they reach the service layer.

The following are rejected with:

```text
400 Bad Request
```

- Missing URLs
- Empty URLs
- Whitespace-only URLs
- Malformed URLs
- URLs exceeding 2048 characters
- Unsupported schemes such as `ftp`
- `javascript:` URLs

Only absolute `http` and `https` URLs are accepted.

### Short Code Uniqueness

Each shortened URL is assigned a unique `ShortCode`.

Uniqueness is enforced at both the application and database levels.

The application checks whether a generated code already exists before saving, while PostgreSQL enforces uniqueness through a unique index.

The database constraint provides the final guarantee against duplicate short codes, including concurrent requests or multiple application instances.

If a database-level collision occurs, the service retries with a newly generated short code.

### Redirect to URL

```http
GET /{shortCode}
```

Redirects the user to the original URL associated with the short code.

A successful redirect returns:

```text
302 Found
```

A short code that does not exist returns:

```text
404 Not Found
```

### Redirect Behaviour

The service uses **HTTP 302 (Temporary Redirect)** rather than 301 (Permanent Redirect).

A 302 avoids clients and caches treating the destination as permanently associated with the short URL, allowing the destination to be changed in the future if required.

### Redis Caching

Redis is used as a cache for shortened URL destinations.

The service follows a **cache-aside** approach:

1. Check Redis for the short code.
2. If the URL is cached, use the cached destination.
3. If the URL is not cached, retrieve it from PostgreSQL.
4. Store the destination in Redis.
5. Return the destination.

PostgreSQL remains the source of truth for URL data and click counts.

### Redis Resilience

Redis is treated as an optimisation rather than a required dependency for serving redirects.

If a Redis read fails, the service falls back to PostgreSQL.

If a Redis write fails after successfully retrieving the URL from PostgreSQL, the request still succeeds and the URL is returned without caching the result.

This prevents a Redis outage from unnecessarily making URL redirection unavailable.

### Asynchronous Click Tracking

Click counts are not updated synchronously during the redirect request.

When a valid short code is redirected, the API creates a `UrlClickedEvent` and persists it in the PostgreSQL outbox as part of a transaction.

The outbox publisher subsequently publishes the event to Kafka.

The consumer then processes the event asynchronously and increments the PostgreSQL click count.

This removes the click-count database write and Kafka delivery latency from the latency-sensitive redirect path.

### Click Event Idempotency

Kafka consumers need to account for duplicate message delivery.

The application assigns every click event a unique `EventId`.

Before processing a click event, the consumer checks the `ProcessedClickEvents` table.

If the event has already been processed, it is skipped.

Otherwise:

```text
1. Increment ClickCount atomically
2. Record EventId in ProcessedClickEvents
3. Commit the database transaction
4. Commit the Kafka offset
```

The database transaction ensures that the click update and event-recording operation succeed together.

This prevents the same Kafka event from incrementing the click count more than once.

### Click Count Concurrency

The click count is updated atomically at the database level:

```text
ClickCount = ClickCount + 1
```

This avoids lost updates when multiple events attempt to increment the same URL concurrently.

The click count update is performed by the asynchronous click-event processor rather than directly on the redirect request path.

### Rate Limiting

The API uses endpoint-specific rate limiting to protect against excessive requests.

The current limits are:

| Endpoint           |                             Limit |
| ------------------ | --------------------------------: |
| `POST /api/urls`   |  5 requests per minute per client |
| `GET /{shortCode}` | 60 requests per minute per client |

Requests exceeding the configured limit return:

```text
429 Too Many Requests
```

Rate limiting is implemented using ASP.NET Core's built-in rate-limiting middleware.

### Logging

The service uses ASP.NET Core's built-in `ILogger` abstraction for structured application logging.

Logs are generated for important application events, including:

- Short URL creation
- Redis cache hits and misses
- URL redirects
- Outbox message creation
- Outbox message claims
- Outbox publishing
- Outbox publishing failures
- Kafka retries
- Kafka publish failures
- Kafka circuit-breaker transitions
- Kafka consumer startup and shutdown
- Kafka processing failures
- Unknown short codes
- Short-code collisions
- Redis read failures
- Redis write failures
- Duplicate click events

Structured logging is used so operational properties such as `ShortCode`, `EventId`, Kafka partition, Kafka offset, retry attempts, and outbox message identifiers can be captured as structured fields rather than embedded directly into log messages.

Sensitive information, including credentials and unnecessary request data, is not logged.

---

## Docker

The application is containerised using a multi-stage Docker build.

The build stage uses the .NET SDK image to restore, build, and publish the application.

The runtime stage uses Microsoft's **.NET 10 Ubuntu Chiseled** ASP.NET runtime image, providing a minimal runtime environment with a reduced attack surface compared with a full Linux runtime image.

The development stack can be started using Docker Compose:

```bash
docker compose --env-file .env.local up --build
```

This runs:

- ASP.NET Core API
- PostgreSQL
- Redis
- Kafka
- OpenTelemetry Collector
- Jaeger
- Prometheus
- Grafana

The API is exposed on:

```text
http://localhost:8080
```

The health endpoint can be checked with:

```bash
curl http://localhost:8080/healthz
```

Kafka uses separate listeners for host and container communication:

```text
Docker containers -> kafka:9092
Host applications  -> localhost:9093
```

Grafana persists its application state using a Docker volume so dashboards, users, and datasource configuration survive container recreation.

---

## CI/CD

GitHub Actions automatically validates changes through the following pipeline:

1. Restore .NET dependencies.
2. Build the application in Release configuration.
3. Run the automated test suite.
4. Build the Docker image.
5. Scan the Docker image with **Trivy** for HIGH and CRITICAL vulnerabilities with available fixes.
6. For pushes to `main`, authenticate to AWS using **GitHub Actions OIDC**.
7. Publish the Docker image to **Amazon ECR**.

AWS credentials are not stored in the repository. GitHub Actions assumes a dedicated IAM role using OIDC.

Docker images pushed to ECR use the Git commit SHA as their tag, providing immutable and traceable image versions.

AWS application deployment is not currently part of the CI/CD pipeline.

---

## Tech Stack

| Technology              | Purpose                                           |
| ----------------------- | ------------------------------------------------- |
| C# / .NET 10            | Backend development                               |
| ASP.NET Core            | REST API                                          |
| Entity Framework Core   | Data access                                       |
| PostgreSQL              | Primary database                                  |
| Redis                   | Distributed caching                               |
| Apache Kafka            | Event streaming and asynchronous click processing |
| Confluent.Kafka         | .NET Kafka client                                 |
| Polly                   | Retry and circuit-breaker policies                |
| OpenTelemetry           | Metrics and distributed tracing                   |
| Prometheus              | Metrics collection and querying                   |
| Grafana                 | Metrics dashboards                                |
| OpenTelemetry Collector | Telemetry collection and trace forwarding         |
| Jaeger                  | Distributed trace visualisation                   |
| xUnit                   | Unit and integration testing                      |
| Moq                     | Dependency mocking                                |
| Testcontainers          | Database integration testing                      |
| Docker                  | Containerisation                                  |
| Docker Compose          | Local service orchestration                       |
| k6                      | Load and performance testing                      |
| Trivy                   | Container vulnerability scanning                  |
| GitHub Actions          | CI/CD automation                                  |
| AWS ECR                 | Container image registry                          |
| Kubernetes              | Container orchestration _(planned)_               |
| AWS                     | Cloud infrastructure and deployment               |

---

## Testing

The project uses multiple levels of automated testing:

- **Unit tests** for service and controller behaviour.
- **Integration tests** for API behaviour and database persistence.
- **Kafka integration tests** for event publication and consumer processing.
- **Resilience tests** for Kafka retry and failure behaviour.
- **Circuit-breaker tests** for circuit opening and recovery.
- **Transactional outbox tests** for durable event handling.
- **Concurrency tests** for simultaneous requests and publisher instances.
- **Testcontainers** to run PostgreSQL during integration testing.
- **Moq** to isolate Redis and Kafka producer dependencies where appropriate.

The test suite verifies functionality including:

- URL creation
- Valid URL validation
- Missing URL validation
- Empty URL validation
- Whitespace-only URL validation
- Malformed URL validation
- HTTP/HTTPS scheme validation
- Unsupported URL scheme rejection
- `javascript:` URL rejection
- Maximum URL length validation
- Short-code generation
- Short-code uniqueness
- Database-level collision handling
- URL redirection
- HTTP 302 responses
- Click-event creation
- Click-event publication
- Click-count tracking
- Concurrent click-count updates
- Kafka consumer processing
- Kafka event idempotency
- Non-existent short codes
- Redis cache behaviour
- Redis read failure fallback to PostgreSQL
- Redis write failure resilience
- Redis failure logging
- PostgreSQL persistence
- Rate limiting for URL creation
- Rate limiting for redirects
- `429 Too Many Requests` responses
- Kafka retry behaviour
- Kafka publish failure handling
- Circuit-breaker opening
- Circuit-breaker fail-fast behaviour
- Circuit-breaker recovery
- Transactional outbox persistence
- Outbox message claiming
- Concurrent outbox publisher claiming
- Stale outbox claim recovery
- Failed outbox publication
- Outbox retry after failure
- Kafka failure and recovery
- End-to-end API behaviour

### Kafka Consumer Integration Tests

Kafka consumer integration tests verify the real message path:

```text
Kafka
  |
  v
ClickEventConsumer
  |
  v
ClickEventProcessor
  |
  v
PostgreSQL
```

Test Kafka topics are isolated from the production topic to prevent retained historical messages from interfering with test execution.

Run the complete test suite with:

```bash
dotnet test
```

---

## Observability

The application uses OpenTelemetry to provide metrics and distributed tracing.

The local observability stack consists of:

```text
Application
    |
    +-- Metrics --> Prometheus --> Grafana
    |
    +-- Traces --> OpenTelemetry Collector --> Jaeger
```

### Metrics

The API exposes Prometheus-compatible metrics through:

```text
http://localhost:8080/metrics
```

Built-in metrics include:

- ASP.NET Core HTTP request metrics
- .NET runtime metrics
- .NET process metrics

Custom application metrics include:

```text
urlshortener_click_events_published_total
urlshortener_click_events_processed_total
urlshortener_click_events_duplicates_total
urlshortener_click_events_publish_failures_total
urlshortener_click_events_publish_retries_total

urlshortener_kafka_publish_duration_milliseconds
urlshortener_click_events_processing_duration_milliseconds

urlshortener_kafka_circuit_opened_total
urlshortener_kafka_circuit_half_opened_total
urlshortener_kafka_circuit_closed_total

urlshortener_outbox_backlog
urlshortener_outbox_oldest_age
urlshortener_outbox_published_total
urlshortener_outbox_publish_failures_total
urlshortener_outbox_publish_duration_milliseconds
```

These metrics provide visibility into:

- Kafka publishing throughput
- Kafka retries
- Kafka failures
- Kafka publish latency
- Click-event processing latency
- Duplicate events
- Kafka circuit-breaker transitions
- Click-event processing throughput
- Outbox backlog
- Outbox message age
- Outbox publication throughput
- Outbox publication failures
- Outbox publication latency

### Grafana

Grafana is available at:

```text
http://localhost:3100
```

The dashboard provides visibility into:

- Click events published
- Click events processed
- Duplicate click events
- Kafka publish failures
- Kafka publish retries
- Kafka publish throughput
- Kafka failure rate
- Kafka publish latency
- Click processing latency
- Published vs processed events
- Unprocessed events
- Kafka circuit-breaker transitions
- Outbox backlog
- Oldest unpublished outbox message age
- Outbox publication rate
- Outbox publication failures
- Outbox publish latency

The dashboard is designed to make dependency failures, event backlog, and recovery behaviour visible during local testing.

### Prometheus

Prometheus is available at:

```text
http://localhost:9090
```

The API is scraped through the Docker Compose network using:

```text
api:8080/metrics
```

The current scrape interval is 5 seconds.

### Distributed Tracing

The application creates custom spans for Kafka publishing and click-event processing.

Trace context is propagated through Kafka message headers so the producer and consumer spans can be associated with the same distributed trace.

The tracing flow is:

```text
HTTP request
    |
    +-- Redis operation
    |
    +-- Outbox persistence
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

Jaeger is available at:

```text
http://localhost:16686
```

### Observability Documentation

Detailed setup instructions, metric definitions, PromQL examples, tracing information, resilience metrics, dashboard information, and troubleshooting guidance are available in:

```text
src/UrlShortenerBackend/Observability/README.md
```

---

## Load Testing

Load and performance testing is performed using [k6](https://k6.io/).

Load-test scripts, benchmark results, and instructions for running the performance tests are available in:

```text
load-tests/README.md
```

Initial benchmarking identified synchronous click-count persistence as a key performance bottleneck under concurrent load.

After moving click-count persistence to asynchronous Kafka processing, the redirect benchmark improved substantially under the same test configuration.

The recorded benchmark comparison is:

| Metric          | Before Kafka |  After Kafka |
| --------------- | -----------: | -----------: |
| Throughput      |   ~207 req/s | ~2,173 req/s |
| Average latency |    120.50 ms |     11.38 ms |
| p95 latency     |    330.09 ms |     14.06 ms |
| Error rate      |           0% |           0% |

This represents approximately:

- **10.5× higher throughput**
- **10.6× lower average latency**
- **23.5× lower p95 latency**

The benchmark measures the application path as a whole, including event persistence/publication, so the improvement represents the effect of the asynchronous architecture rather than Kafka alone.

### Resilience Testing

The application has also been tested under Kafka failure conditions.

Kafka can be stopped during local testing:

```bash
docker stop urlshortener-kafka
```

During the outage:

```text
Redirect request
      |
      v
PostgreSQL transaction
      |
      +-- Outbox event persisted
      |
      v
302 response

Kafka publishing
      |
      +-- retry
      |
      +-- circuit opens
      |
      v
Outbox remains durable in PostgreSQL
```

When Kafka is restarted:

```text
Kafka returns
      |
      v
OutboxPublisher retries
      |
      v
Backlog drains
      |
      v
ClickEventConsumer processes events
      |
      v
Click counts catch up
```

Grafana metrics can be used to observe:

- Outbox backlog growth
- Outbox oldest-message age
- Retry attempts
- Publish failures
- Increased Kafka publish latency
- Circuit-breaker opening
- Circuit half-open transitions
- Circuit recovery
- Outbox backlog recovery

---

## Project Status

🚧 **In development**

The project has progressed from a basic URL-shortening API into a distributed, observable, resilient backend architecture with PostgreSQL, Redis, Kafka, a transactional outbox, automated testing, Docker, OpenTelemetry, Grafana, AWS ECR, and CI/CD automation.

### Completed

- REST API
- PostgreSQL persistence
- Entity Framework Core
- Database migrations
- Short-code generation and uniqueness enforcement
- Database-level collision handling
- Service layer
- Unit testing
- Integration testing
- Kafka integration testing
- Concurrency testing
- Request validation
- HTTP/HTTPS URL scheme validation
- Maximum URL length validation
- Consistent `400 Bad Request` validation responses
- Global ASP.NET Core `ProblemDetails` exception handling
- `201 Created` response for successful URL creation
- `302 Found` redirects
- `404 Not Found` handling for unknown short codes
- Endpoint-specific rate limiting
- Rate limiting for URL creation and redirects
- `429 Too Many Requests` responses
- Rate-limiting integration tests
- Redis cache-aside implementation
- Redis failure resilience and PostgreSQL fallback
- Redis failure resilience tests
- Structured application logging
- Structured logging for URL lifecycle events and Redis failures
- Logging tests for Redis failure scenarios
- Kafka event creation
- Kafka consumer
- Asynchronous click-count persistence
- Atomic click-count updates
- Kafka event idempotency
- Database-backed processed-event tracking
- Concurrent click-count correctness testing
- Kafka consumer integration testing
- Kafka producer unit testing
- Kafka retry and exponential backoff
- Kafka publish failure handling
- Kafka circuit breaker
- Circuit-breaker unit tests
- Circuit-breaker recovery testing
- Kafka failure and recovery testing with Docker
- Transactional outbox implementation
- Durable click-event persistence
- PostgreSQL-backed outbox message claiming
- Concurrent outbox publisher protection with `FOR UPDATE SKIP LOCKED`
- Stale outbox claim recovery
- Outbox publish retry handling
- Outbox failure handling
- Outbox metrics
- Outbox backlog and oldest-event monitoring
- k6 load-testing infrastructure
- Initial performance benchmarking
- Performance bottleneck identification
- Post-Kafka performance benchmarking
- OpenTelemetry metrics
- OpenTelemetry distributed tracing
- Custom Kafka application metrics
- Kafka resilience metrics
- Prometheus metrics collection
- Grafana dashboards
- Persistent Grafana storage
- Outbox Grafana monitoring
- OpenTelemetry Collector
- Jaeger distributed tracing
- Kafka trace-context propagation
- Dockerised PostgreSQL
- Dockerised Redis
- Dockerised Kafka
- Dockerised ASP.NET Core API
- Dockerised observability stack
- Multi-stage Docker build
- Minimal/chiseled .NET runtime image
- Docker Compose infrastructure
- Host and container Kafka listener configuration
- Health checks
- Swagger/OpenAPI
- GitHub Actions CI pipeline
- Docker image vulnerability scanning with Trivy
- AWS CLI and development account setup
- Amazon ECR repository
- GitHub Actions OIDC authentication with AWS
- Immutable Git SHA Docker image tagging
- Automated Docker image publishing to ECR

### Planned

- Kafka consumer lag monitoring
- Kafka retry and event-delivery alerting
- Kafka consumer health metrics
- Liveness and readiness endpoints
- PostgreSQL tracing instrumentation
- Grafana dashboard provisioning
- Higher-concurrency load testing
- Performance optimisation
- Security hardening
- AWS application deployment
- AWS networking architecture
- Managed PostgreSQL deployment
- Managed Redis deployment
- Kubernetes deployment
- Distributed system scalability testing
- Expanded resilience and fault-tolerance testing
- Cloud-based monitoring and alerting

The project will progressively evolve towards a **distributed, scalable, observable, resilient, and production-oriented backend system**.
