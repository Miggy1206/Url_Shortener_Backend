using System.Net;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using UrlShortenerBackend.Api.Data;

namespace UrlShortenerBackend.Tests.Integration;

public class HealthCheckTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture _postgres;

    public HealthCheckTests(PostgresFixture postgres)
    {
        _postgres = postgres;
    }

    private WebApplicationFactory<Program> CreateFactory()
    {
        return new WebApplicationFactory<Program>()
            .WithWebHostBuilder(builder =>
            {
                builder.ConfigureServices(services =>
                {
                    var descriptor =
                        services.SingleOrDefault(
                            service =>
                                service.ServiceType ==
                                typeof(
                                    DbContextOptions<
                                        UrlShortenerDbContext>));

                    if (descriptor is not null)
                    {
                        services.Remove(descriptor);
                    }

                    services.AddDbContext<UrlShortenerDbContext>(
                        options =>
                            options.UseNpgsql(
                                _postgres.ConnectionString));
                });
            });
    }

    [Fact]
    public async Task LiveEndpoint_WhenApplicationIsRunning_ReturnsOk()
    {
        using var factory = CreateFactory();
        using var client = factory.CreateClient();

        var response =
            await client.GetAsync("/health/live");

        Assert.Equal(
            HttpStatusCode.OK,
            response.StatusCode);
    }

    [Fact]
    public async Task ReadyEndpoint_WhenPostgresIsAvailable_ReturnsOk()
    {
        using var factory = CreateFactory();
        using var client = factory.CreateClient();

        var response =
            await client.GetAsync("/health/ready");

        Assert.Equal(
            HttpStatusCode.OK,
            response.StatusCode);
    }

    [Fact]
    public async Task LiveEndpoint_DoesNotDependOnPostgres_ReturnsOk()
    {
        using var factory =
            new WebApplicationFactory<Program>();

        using var client =
            factory.CreateClient();

        var response =
            await client.GetAsync("/health/live");

        Assert.Equal(
            HttpStatusCode.OK,
            response.StatusCode);
    }

    [Fact]
    public async Task ReadyEndpoint_WhenPostgresIsUnavailable_ReturnsServiceUnavailable()
    {
        using var factory =
            new WebApplicationFactory<Program>()
                .WithWebHostBuilder(builder =>
                {
                    builder.ConfigureServices(services =>
                    {
                        var descriptor =
                            services.SingleOrDefault(
                                service =>
                                    service.ServiceType ==
                                    typeof(
                                        DbContextOptions<
                                            UrlShortenerDbContext>));

                        if (descriptor is not null)
                        {
                            services.Remove(descriptor);
                        }

                        services.AddDbContext<UrlShortenerDbContext>(
                            options =>
                                options.UseNpgsql(
                                    "Host=localhost;" +
                                    "Port=59999;" +
                                    "Database=does-not-exist;" +
                                    "Username=postgres;" +
                                    "Password=invalid;" +
                                    "Timeout=1;" +
                                    "Command Timeout=1"));
                    });
                });

        using var client =
            factory.CreateClient();

        var response =
            await client.GetAsync("/health/ready");

        Assert.Equal(
            HttpStatusCode.ServiceUnavailable,
            response.StatusCode);
    }
}
