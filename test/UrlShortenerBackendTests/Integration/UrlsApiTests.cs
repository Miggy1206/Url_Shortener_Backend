using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using UrlShortenerBackend.Api.Data;
using UrlShortenerBackend.Api.Models;

namespace UrlShortenerBackend.Tests.Integration;

public class UrlsApiTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture _postgres;
    private readonly HttpClient _client;

    public UrlsApiTests(PostgresFixture postgresFixture)
    {
        _postgres = postgresFixture;

        var factory = new ApiFactory(postgresFixture);

        _client = factory.CreateClient(
            new WebApplicationFactoryClientOptions
            {
                AllowAutoRedirect = false
            });
    }

    [Fact]
    public async Task CreateShortUrl_WithValidUrl_ReturnsOk()
    {
        // Arrange
        var request = new
        {
            originalUrl = "https://www.example.com"
        };

        // Act
        var response = await _client.PostAsJsonAsync(
            "/api/urls",
            request);

        // Assert
        Assert.Equal(
            HttpStatusCode.Created,
            response.StatusCode);
    }

    [Fact]
    public async Task CreateAndRedirectUrl_WorksEndToEnd()
    {
        // Arrange
        var request = new
        {
            originalUrl = "https://www.example.com"
        };

        // Act - Create short URL
        var createResponse = await _client.PostAsJsonAsync(
            "/api/urls",
            request);

        // Assert - Creation
        Assert.Equal(
            HttpStatusCode.Created,
            createResponse.StatusCode);

        var createResult =
            await createResponse.Content.ReadFromJsonAsync<JsonElement>();

        var shortCode = createResult
            .GetProperty("shortCode")
            .GetString();

        Assert.NotNull(shortCode);

        // Act - Follow the short URL
        var redirectResponse = await _client.GetAsync(
            $"/{shortCode}");

        // Assert - Redirect
        Assert.Equal(
            HttpStatusCode.Redirect,
            redirectResponse.StatusCode);

        Assert.Equal(
            "https://www.example.com/",
            redirectResponse.Headers.Location?.ToString());

        // Assert - Kafka consumer eventually persists click
        await WaitForClickCountAsync(
            shortCode,
            1);
    }

    [Fact]
    public async Task ShortenUrl_WithMissingUrl_ReturnsBadRequest()
    {
        // Arrange
        var request = new
        {
            OriginalUrl = ""
        };

        // Act
        var response = await _client.PostAsJsonAsync(
            "/api/urls",
            request);

        // Assert
        Assert.Equal(
            HttpStatusCode.BadRequest,
            response.StatusCode);
    }

    [Fact]
    public async Task ShortenUrl_WithInvalidUrl_ReturnsBadRequest()
    {
        // Arrange
        var request = new
        {
            OriginalUrl = "not-a-url"
        };

        // Act
        var response = await _client.PostAsJsonAsync(
            "/api/urls",
            request);

        // Assert
        Assert.Equal(
            HttpStatusCode.BadRequest,
            response.StatusCode);
    }

    [Fact]
    public async Task ShortenUrl_WithUnsupportedScheme_ReturnsBadRequest()
    {
        // Arrange
        var request = new
        {
            originalUrl = "ftp://example.com/file.txt"
        };

        // Act
        var response = await _client.PostAsJsonAsync(
            "/api/urls",
            request);

        // Assert
        Assert.Equal(
            HttpStatusCode.BadRequest,
            response.StatusCode);
    }

    [Fact]
    public async Task ShortenUrl_WithJavascriptUrl_ReturnsBadRequest()
    {
        // Arrange
        var request = new
        {
            originalUrl = "javascript:alert(1)"
        };

        // Act
        var response = await _client.PostAsJsonAsync(
            "/api/urls",
            request);

        // Assert
        Assert.Equal(
            HttpStatusCode.BadRequest,
            response.StatusCode);
    }

    [Fact]
    public async Task ShortenUrl_WithUrlExceedingMaxLength_ReturnsBadRequest()
    {
        // Arrange
        var longUrl =
            $"https://example.com/{new string('a', 2048)}";

        var request = new
        {
            originalUrl = longUrl
        };

        // Act
        var response = await _client.PostAsJsonAsync(
            "/api/urls",
            request);

        // Assert
        Assert.Equal(
            HttpStatusCode.BadRequest,
            response.StatusCode);
    }

    [Fact]
    public async Task CreateShortUrl_WhenRateLimitExceeded_ReturnsTooManyRequests()
    {
        // Arrange & Act
        for (var i = 0; i < 5; i++)
        {
            var request = new
            {
                originalUrl =
                    $"https://example.com/rate-limit/{Guid.NewGuid()}"
            };

            var response = await _client.PostAsJsonAsync(
                "/api/urls",
                request);

            Assert.Equal(
                HttpStatusCode.Created,
                response.StatusCode);
        }

        var limitedRequest = new
        {
            originalUrl =
                $"https://example.com/rate-limit/{Guid.NewGuid()}"
        };

        var limitedResponse = await _client.PostAsJsonAsync(
            "/api/urls",
            limitedRequest);

        // Assert
        Assert.Equal(
            HttpStatusCode.TooManyRequests,
            limitedResponse.StatusCode);
    }

    [Fact]
    public async Task RedirectToUrl_WhenRateLimitExceeded_ReturnsTooManyRequests()
    {
        // Arrange
        var request = new
        {
            originalUrl =
                $"https://example.com/rate-limit/{Guid.NewGuid()}"
        };

        var createResponse = await _client.PostAsJsonAsync(
            "/api/urls",
            request);

        Assert.Equal(
            HttpStatusCode.Created,
            createResponse.StatusCode);

        var createResult =
            await createResponse.Content.ReadFromJsonAsync<JsonElement>();

        var shortCode = createResult
            .GetProperty("shortCode")
            .GetString();

        Assert.NotNull(shortCode);

        // Act
        for (var i = 0; i < 60; i++)
        {
            var response = await _client.GetAsync(
                $"/{shortCode}");

            Assert.Equal(
                HttpStatusCode.Redirect,
                response.StatusCode);
        }

        var limitedResponse = await _client.GetAsync(
            $"/{shortCode}");

        // Assert
        Assert.Equal(
            HttpStatusCode.TooManyRequests,
            limitedResponse.StatusCode);
    }

    [Fact]
    public async Task RedirectUrl_WithConcurrentRequests_PersistsAllClicks()
    {
        // Arrange
        var request = new
        {
            originalUrl = "https://www.example.com"
        };

        var createResponse = await _client.PostAsJsonAsync(
            "/api/urls",
            request);

        Assert.Equal(
            HttpStatusCode.Created,
            createResponse.StatusCode);

        var createResult =
            await createResponse.Content.ReadFromJsonAsync<JsonElement>();

        var shortCode = createResult
            .GetProperty("shortCode")
            .GetString();

        Assert.NotNull(shortCode);

        const int requestCount = 50;

        // Act
        var tasks = Enumerable.Range(0, requestCount)
            .Select(_ =>
                _client.GetAsync($"/{shortCode}"));

        var responses = await Task.WhenAll(tasks);

        // Assert - all requests successfully produced redirects
        Assert.All(
            responses,
            response => Assert.Equal(
                HttpStatusCode.Redirect,
                response.StatusCode));

        // Assert - Kafka consumer eventually persists all clicks
        await WaitForClickCountAsync(
            shortCode,
            requestCount);
    }

    private async Task WaitForClickCountAsync(
        string shortCode,
        int expectedClickCount,
        TimeSpan? timeout = null)
    {
        var deadline =
            DateTime.UtcNow +
            (timeout ?? TimeSpan.FromSeconds(10));

        while (DateTime.UtcNow < deadline)
        {
            await using var context = CreateDbContext();

            var clickCount = await context.Urls
                .Where(x => x.ShortCode == shortCode)
                .Select(x => x.ClickCount)
                .SingleAsync();

            if (clickCount == expectedClickCount)
            {
                return;
            }

            await Task.Delay(100);
        }

        await using var finalContext = CreateDbContext();

        var finalClickCount = await finalContext.Urls
            .Where(x => x.ShortCode == shortCode)
            .Select(x => x.ClickCount)
            .SingleAsync();

        Assert.Equal(
            expectedClickCount,
            finalClickCount);
    }

    private UrlShortenerDbContext CreateDbContext()
    {
        var options =
            new DbContextOptionsBuilder<UrlShortenerDbContext>()
                .UseNpgsql(_postgres.ConnectionString)
                .Options;

        return new UrlShortenerDbContext(options);
    }
}