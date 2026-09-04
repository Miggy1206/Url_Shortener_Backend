using Microsoft.EntityFrameworkCore;
using Testcontainers.PostgreSql;
using UrlShortenerBackend.Api.Data;

namespace UrlShortenerBackend.Tests.Integration;

public class PostgresFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres =
        new PostgreSqlBuilder("postgres:18")
            .Build();

    public string ConnectionString => _postgres.GetConnectionString();

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();

        var options = new DbContextOptionsBuilder<UrlShortenerDbContext>()
            .UseNpgsql(ConnectionString)
            .Options;

        await using var context = new UrlShortenerDbContext(options);

        await context.Database.MigrateAsync();
    }

    public async Task DisposeAsync()
    {
        await _postgres.DisposeAsync();
    }
}