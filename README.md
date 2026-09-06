# URL Shortener Backend

A distributed URL shortening service built with **C# and .NET 10**.

The project is designed as a practical exploration of modern backend and distributed systems engineering, progressing from a simple API into a scalable, production-oriented service.

The focus is on understanding how distributed services are designed, tested, containerised, deployed, and scaled, while exploring technologies such as PostgreSQL, Redis, Docker, AWS, and Kubernetes.

## Table of Contents

- [Objectives](#objectives)
- [Setup](#setup)
- [API](#api)
- [Docker](#docker)
- [CI/CD](#cicd)
- [Tech Stack](#tech-stack)
- [Testing](#testing)
- [Project Status](#project-status)

## Objectives

The main objectives of this project are to:

- Build a robust REST API using **ASP.NET Core and .NET 10**.
- Explore **distributed systems architecture**, scalability, availability, and fault tolerance.
- Develop practical experience with **PostgreSQL and Entity Framework Core**.
- Use **Redis** for distributed caching and performance optimisation.
- Apply automated **unit and integration testing** throughout development.
- Learn containerisation and service orchestration using **Docker and Kubernetes**.
- Explore **AWS** and cloud-based infrastructure.
- Understand concepts such as **load balancing, service communication, caching, concurrency, observability, and resilience**.
- Apply software engineering principles around **architecture, maintainability, scalability, security, and performance**.

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

Start PostgreSQL and Redis using Docker Compose:

```bash
docker compose --env-file .env.local up -d
```

PostgreSQL is exposed locally on port `5433`, while Redis is available on port `6379`.

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

### Apply Database Migrations

```bash
dotnet ef database update
```

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

The test suite includes unit tests and integration tests using a real PostgreSQL database through Testcontainers.

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

This prevents a Redis outage from unnecessarily making the URL redirection functionality unavailable.

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
- Unknown short codes
- Short-code collisions
- Redis read failures
- Redis write failures

Structured logging is used so operational properties such as `ShortCode` and retry attempts can be captured as structured fields rather than embedded directly into log messages.

Sensitive information, including credentials and unnecessary request data, is not logged.

The logging implementation is designed to integrate with cloud-based observability platforms such as AWS CloudWatch when the application is deployed to AWS.

## Docker

The application is containerised using a multi-stage Docker build.

The build stage uses the .NET SDK image to restore, build, and publish the application.

The runtime stage uses Microsoft's **.NET 10 Ubuntu Chiseled** ASP.NET runtime image, providing a minimal runtime environment with a reduced attack surface compared with a full Linux runtime image.

The complete development stack can be started using Docker Compose:

```bash
docker compose --env-file .env.local up --build
```

This runs:

- ASP.NET Core API
- PostgreSQL
- Redis

The API is exposed on:

```text
http://localhost:8080
```

The health endpoint can be checked with:

```bash
curl http://localhost:8080/healthz
```

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

## Tech Stack

| Technology            | Purpose                             |
| --------------------- | ----------------------------------- |
| C# / .NET 10          | Backend development                 |
| ASP.NET Core          | REST API                            |
| Entity Framework Core | Data access                         |
| PostgreSQL            | Primary database                    |
| Redis                 | Distributed caching                 |
| xUnit                 | Unit and integration testing        |
| Moq                   | Dependency mocking                  |
| Testcontainers        | Database integration testing        |
| Docker                | Containerisation                    |
| Docker Compose        | Local service orchestration         |
| Trivy                 | Container vulnerability scanning    |
| GitHub Actions        | CI/CD automation                    |
| AWS ECR               | Container image registry            |
| Kubernetes            | Container orchestration _(planned)_ |
| AWS                   | Cloud infrastructure and deployment |

## Testing

The project uses multiple levels of automated testing:

- **Unit tests** for service and controller behaviour.
- **Integration tests** for API behaviour and database persistence.
- **Testcontainers** to run PostgreSQL during integration tests.
- **Moq** to isolate Redis dependencies in unit tests.

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
- URL redirection
- HTTP 302 responses
- Click-count tracking
- Non-existent short codes
- Redis cache behaviour
- Redis read failure fallback to PostgreSQL
- Redis write failure resilience
- Redis failure logging
- PostgreSQL persistence
- Rate limiting for URL creation
- Rate limiting for redirects
- `429 Too Many Requests` responses
- End-to-end API behaviour

Run the complete test suite with:

```bash
dotnet test
```

## Project Status

🚧 **In development**

The initial API and database foundation have been implemented alongside a service layer, automated testing, Redis caching, Docker infrastructure, CI/CD automation, security scanning, structured logging, and AWS container registry integration.

### Completed

- REST API
- PostgreSQL persistence
- Entity Framework Core
- Database migrations
- Short-code generation and uniqueness enforcement
- Service layer
- Unit testing
- Integration testing
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
- Dockerised PostgreSQL
- Dockerised Redis
- Dockerised ASP.NET Core API
- Multi-stage Docker build
- Minimal/chiseled .NET runtime image
- Docker Compose infrastructure
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

- Concurrency testing
- Click-count concurrency improvements
- Metrics, dashboards, and distributed tracing
- AWS application deployment
- AWS networking architecture
- Managed PostgreSQL deployment
- Managed Redis deployment
- Load testing
- Performance benchmarking and optimisation
- Security hardening
- Kubernetes deployment
- Distributed system scalability
- Resilience and fault-tolerance patterns

The project will progressively evolve towards a **distributed, scalable, observable, and production-oriented backend system**.
