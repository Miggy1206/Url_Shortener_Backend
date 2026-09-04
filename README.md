# URL Shortener Backend

A distributed URL shortening service built with **C# and .NET 10**.

The project is designed as a practical exploration of modern backend and distributed systems engineering, progressing from a simple API into a scalable, production-oriented service.

The focus is on understanding how distributed services are designed, tested, containerised, deployed, and scaled, while exploring technologies such as PostgreSQL, Redis, Docker, AWS, and Kubernetes.

## Table of Contents

- [Objectives](#objectives)
- [Setup](#setup)
- [API](#api)
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

### Configure PostgreSQL and Redis

Create a local `.env.local` file for Docker Compose:

```env
POSTGRES_DB=urlshortener
POSTGRES_USER=postgres
POSTGRES_PASSWORD=<your-password>
```

Start the PostgreSQL and Redis containers:

```bash
docker compose --env-file .env.local up -d
```

PostgreSQL is exposed locally on port `5433`, while Redis is available on port `6379`.

### Configure the API

Configure the local database connection using .NET User Secrets:

```bash
cd src/UrlShortenerBackend

dotnet user-secrets init

dotnet user-secrets set "ConnectionStrings:DefaultConnection" "Host=localhost;Port=5433;Database=urlshortener;Username=postgres;Password=<your-password>"
```

The API connects to Redis using:

```text
localhost:6379
```

### Apply Database Migrations

```bash
dotnet ef database update
```

### Run the API

```bash
dotnet run
```

Swagger can then be used to interact with the API.

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

### Short Code Uniqueness

Each shortened URL is assigned a unique `ShortCode`.

Uniqueness is enforced at both the application and database levels.

The application checks whether a generated code already exists before saving, while PostgreSQL enforces uniqueness through a unique index:

```csharp
modelBuilder.Entity<Url>()
    .HasIndex(x => x.ShortCode)
    .IsUnique();
```

The database constraint provides the final guarantee against duplicate short codes, including concurrent requests or multiple application instances.

If a database-level collision occurs, the service retries with a newly generated short code.

### Redirect to URL

```http
GET /{shortCode}
```

Redirects the user to the original URL associated with the short code.

### Redirect Behaviour

The service uses **HTTP 302 (Temporary Redirect)** rather than 301 (Permanent Redirect).

A 302 avoids clients and caches treating the destination as permanently associated with the short URL, allowing the destination to be changed in the future if required.

### Redis Caching

Redis is used as a cache for shortened URL destinations.

The service follows a **cache-aside** approach:

1. Check Redis for the short code.
2. If the URL is cached, return the cached destination.
3. If the URL is not cached, retrieve it from PostgreSQL.
4. Store the destination in Redis.
5. Return the destination.

PostgreSQL remains the source of truth for URL data and click counts.

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
| Kubernetes            | Container orchestration _(planned)_ |
| AWS                   | Cloud infrastructure _(planned)_    |

## Testing

The project uses multiple levels of automated testing:

- **Unit tests** for controller and service behaviour.
- **Integration tests** for API behaviour and database persistence.
- **Testcontainers** to run PostgreSQL during integration tests.
- **Moq** to isolate Redis dependencies in unit tests.

The test suite verifies functionality including:

- URL creation
- Short-code generation
- Short-code uniqueness
- URL redirection
- HTTP 302 responses
- Click-count tracking
- Non-existent short codes
- Redis cache behaviour
- PostgreSQL persistence
- End-to-end API behaviour

Run the complete test suite with:

```bash
dotnet test
```

## Project Status

🚧 **In development**

The initial API and database foundation have been implemented alongside a service layer, automated testing, Redis caching, and Docker-based infrastructure.

### Completed

- REST API
- PostgreSQL persistence
- Entity Framework Core
- Database migrations
- Short-code generation and uniqueness enforcement
- Service layer
- Unit testing
- Integration testing
- Dockerised PostgreSQL
- Dockerised Redis
- Redis cache-aside implementation
- Health checks
- Swagger/OpenAPI

### Planned

- Dockerise the API
- CI/CD pipeline
- AWS deployment
- Kubernetes deployment
- Observability and structured logging
- Resilience and fault-tolerance patterns
- Load testing
- Performance optimisation
- Distributed system scalability
- Production-ready security

The project will progressively evolve towards a **distributed, scalable, observable, and production-oriented backend system**.
