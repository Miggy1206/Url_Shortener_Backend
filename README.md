# URL Shortener Backend

A distributed URL shortening service built with **C# and .NET 10**.

The project is designed as a practical exploration of modern backend and distributed systems engineering, progressing from a simple API into a scalable, observable, resilient, production-oriented service.

The focus is on understanding how distributed services are designed, tested, containerised, deployed, monitored, and scaled, while exploring technologies such as PostgreSQL, Redis, Kafka, Docker, AWS, Kubernetes, and OpenTelemetry.

---

## Table of Contents

- [Objectives](#objectives)
- [Architecture](#architecture)
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
- Apply automated **unit and integration testing** throughout development.
- Learn containerisation and service orchestration using **Docker and Kubernetes**.
- Explore **AWS** and cloud-based infrastructure.
- Implement **observability** using metrics, tracing, dashboards, and structured logging.
- Understand concepts such as **load balancing, service communication, caching, concurrency, messaging, observability, resilience, retries, circuit breakers, and fault isolation**.
- Apply software engineering principles around **architecture, maintainability, scalability, security, and performance**.

---

## Architecture

The application currently consists of an ASP.NET Core API backed by PostgreSQL and Redis, with Kafka used to decouple click-count processing from the redirect request.

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
   |                                      |
   +-- Cache miss -> PostgreSQL            |
                                          |
                                          v
                               Original destination
                                          |
                                          v
                                Publish UrlClickedEvent
                                          |
                                          v
                                  Kafka resilience
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

The redirect request does not wait for PostgreSQL to persist the click count. Click tracking is performed asynchronously through Kafka.

Kafka publishing is protected by retry and circuit-breaker mechanisms so that a Kafka outage does not unnecessarily make the core redirect functionality unavailable.

The Kafka event contains:

```text
EventId
ShortCode
OccurredAt
```

The consumer uses manual Kafka offset commits together with database-backed event idempotency to prevent duplicate delivery from incrementing the click count more than once.

---

## Kafka Delivery Model

The API waits for Kafka to acknowledge the click event before returning the redirect response when the circuit is closed and the publish operation is healthy.

This provides a clear acknowledgement point for the event while keeping the PostgreSQL click-count update asynchronous.

Kafka publishing currently uses two resilience mechanisms:

### Retry and Backoff

Transient publishing failures are retried up to the configured number of attempts using a short exponential backoff.

The current retry sequence is approximately:

```text
Attempt 1
   |
   | failure
   v
short delay
   |
   v
Attempt 2
   |
   | failure
   v
longer delay
   |
   v
Attempt 3
   |
   | failure
   v
Publish failure
```

### Circuit Breaker

Repeated Kafka failures cause the circuit breaker to open.

When the circuit is open:

```text
Redirect request
      |
      v
Circuit OPEN
      |
      +---- Kafka publish skipped
      |
      v
Redirect continues
```

After the configured break duration, the circuit enters half-open and allows a trial request.

A successful trial closes the circuit and Kafka publishing resumes normally.

This prevents prolonged Kafka outages from causing every redirect request to repeatedly wait for Kafka timeouts.

The redirect path deliberately treats click-event persistence as non-critical to serving the destination URL.

A future **transactional outbox** can further improve delivery guarantees by durably storing events before publishing them to Kafka.

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
PostgreSQL → localhost:5433
Redis      → localhost:6379
Kafka      → localhost:9093
Prometheus → localhost:9090
Grafana    → localhost:3100
Jaeger     → localhost:16686
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

dotnet user-secrets set "ConnectionStrings:DefaultConnection" "Host=localhost;Port=5433;Database=urlshortener;Username=postgres;Password=<your-password>"
```

The local API connects to Redis using:

```text
localhost:6379
```

For local Kafka development, the API uses:

```text
localhost:9093
```

For local OpenTelemetry development, the API can send traces to the local OpenTelemetry Collector endpoint when available.

### Apply Database Migrations

```bash
dotnet ef database update
```

### Create the Kafka Topic

The click event topic is:

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

The test suite includes unit tests, database integration tests, concurrency tests, Kafka integration tests, resilience tests, and circuit-breaker tests.

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

Click counts are no longer updated synchronously as part of the redirect request.

When a valid short code is redirected, the API publishes a `UrlClickedEvent` to Kafka.

The consumer then processes the event asynchronously and increments the corresponding PostgreSQL click count.

This removes the PostgreSQL click-count write from the latency-sensitive redirect path.

### Click Event Idempotency

Kafka consumers need to account for the possibility of duplicate message delivery.

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

The click count itself is updated atomically at the database level:

```text
ClickCount = ClickCount + 1
```

This avoids lost updates when multiple events attempt to increment the same URL concurrently.

Atomic database updates are performed by the asynchronous click-event processor rather than directly on the redirect request path.

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
- Kafka click-event publication
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

Structured logging is used so operational properties such as `ShortCode`, `EventId`, Kafka partition, Kafka offset, and retry attempts can be captured as structured fields rather than embedded directly into log messages.

Sensitive information, including credentials and unnecessary request data, is not logged.

The logging implementation is designed to integrate with cloud-based observability platforms when the application is deployed to AWS.

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
Docker containers → kafka:9092
Host applications  → localhost:9093
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
| Polly                   | Resilience and circuit-breaker policies           |
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
- **Testcontainers** to run PostgreSQL during integration tests.
- **Moq** to isolate Redis and Kafka producer dependencies where appropriate.
- **Concurrency tests** to validate correct behaviour under simultaneous requests.

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
- End-to-end API behaviour

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

The dashboard is designed to make dependency failures and resilience behaviour visible during local testing.

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

`src/UrlShortenerBackend/Observability/README.md`

---

## Load Testing

Load and performance testing is performed using [k6](https://k6.io/).

Load-test scripts, benchmark results, and instructions for running the performance tests are available in `load-tests/README.md`.

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

The benchmark measures the application path as a whole, including Kafka event publication, so the improvement represents the effect of the architectural change rather than Kafka alone.

### Resilience Testing

The application has also been tested under Kafka failure conditions.

Kafka can be stopped during local testing:

```bash
docker stop urlshortener-kafka
```

During the outage:

```text
Kafka publish
    |
    +-- retry
    |
    +-- retry
    |
    +-- circuit opens
    |
    v
redirect continues
```

This demonstrates that the redirect path remains available even when Kafka is unavailable.

Grafana metrics can be used to observe:

- Retry attempts
- Publish failures
- Increased Kafka publish latency
- Circuit-breaker opening
- Circuit half-open transitions
- Circuit recovery

---

## Project Status

🚧 **In development**

The initial API and database foundation have been implemented alongside a service layer, automated testing, Redis caching, Docker infrastructure, CI/CD automation, security scanning, structured logging, concurrency handling, Kafka-based asynchronous processing, resilience mechanisms, observability, performance benchmarking, and AWS container registry integration.

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
- Structured logging for important URL lifecycle events and Redis failures
- Logging tests for Redis failure scenarios
- Kafka event publishing
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

- Transactional outbox implementation
- Durable click-event delivery
- Kafka consumer lag monitoring
- Kafka retry and event delivery alerting
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
