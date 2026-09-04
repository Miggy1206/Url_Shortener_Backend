using Microsoft.EntityFrameworkCore;
using Testcontainers.PostgreSql;
using UrlShortenerBackend.Api.Data;
using UrlShortenerBackend.Api.Models;

namespace UrlShortenerBackend.Tests.Integration;

public class UrlsControllerIntegrationTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres =
        new PostgreSqlBuilder("postgres:18")
            .Build();

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _postgres.DisposeAsync();
    }

    [Fact]
    public async Task DatabaseConnection_Works()
    {
        var options = new DbContextOptionsBuilder<UrlShortenerDbContext>()
            .UseNpgsql(_postgres.GetConnectionString())
            .Options;

        await using var context = new UrlShortenerDbContext(options);

        await context.Database.MigrateAsync();

        Assert.True(await context.Database.CanConnectAsync());
    }

    [Fact]
    public async Task ShortenUrl_WithPostgreSql_PersistsUrl()
    {
        var options = new DbContextOptionsBuilder<UrlShortenerDbContext>()
            .UseNpgsql(_postgres.GetConnectionString())
            .Options;

        await using var context = new UrlShortenerDbContext(options);

        await context.Database.MigrateAsync();

        var url = new Url
        {
            OriginalUrl = "https://www.example.com",
            ShortCode = "abc123",
            CreatedAt = DateTime.UtcNow,
            ClickCount = 0
        };

        context.Urls.Add(url);

        await context.SaveChangesAsync();

        var savedUrl = await context.Urls
            .SingleAsync(x => x.ShortCode == "abc123");

        Assert.Equal("https://www.example.com", savedUrl.OriginalUrl);
        Assert.Equal("abc123", savedUrl.ShortCode);
        Assert.Equal(0, savedUrl.ClickCount);
    }
}