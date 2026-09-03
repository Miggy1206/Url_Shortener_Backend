# URL Shortener Backend

A distributed URL shortening service built with **C# and .NET 10**. The project is designed as a practical exploration of modern backend and distributed systems engineering, progressing from a simple API into a scalable, production-oriented service.

The focus is on understanding how distributed services are designed, tested, containerised, deployed, and scaled, while exploring technologies such as PostgreSQL, Redis, Docker, AWS, and Kubernetes.

## Table of Contents

- [Objectives](#objectives)
- [API](#api)
- [Tech Stack](#tech-stack)
- [Project Status](#project-status)

## Objectives

The main objectives of this project are to:

- Build a robust REST API using **ASP.NET Core and .NET 10**.
- Explore **distributed systems architecture**, scalability, availability, and fault tolerance.
- Develop practical experience with **PostgreSQL and Entity Framework Core**.
- Explore **Redis** for distributed caching and performance optimisation.
- Apply automated **unit and integration testing** throughout development.
- Learn containerisation and service orchestration using **Docker and Kubernetes**.
- Explore **AWS** and cloud-based infrastructure.
- Understand concepts such as **load balancing, service communication, caching, concurrency, observability, and resilience**.
- Apply software engineering principles around **architecture, maintainability, scalability, security, and performance**.
- Progressively evolve the application towards a **scalable distributed system**.

## API

### Create Short URL

```http
POST /api/urls
```

Creates a shortened URL from a provided original URL.

## Tech Stack

| Technology            | Purpose                             |
| --------------------- | ----------------------------------- |
| C# / .NET 10          | Backend development                 |
| ASP.NET Core          | REST API                            |
| Entity Framework Core | Data access                         |
| PostgreSQL            | Primary database                    |
| Redis                 | Distributed caching _(planned)_     |
| xUnit                 | Unit testing                        |
| Docker                | Containerisation                    |
| Kubernetes            | Container orchestration _(planned)_ |
| AWS                   | Cloud infrastructure _(planned)_    |

## Project Status

🚧 **In development**

The initial API and database foundation have been implemented. The project will progressively evolve towards a **distributed, scalable, and production-oriented backend system**.
