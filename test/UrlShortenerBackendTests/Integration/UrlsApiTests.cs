using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using UrlShortenerBackend.Api.Data;

namespace UrlShortenerBackend.Tests.Integration;

public class UrlsApiTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture _postgres;
    private readonly HttpClient _client;

    public UrlsApiTests(PostgresFixture postgresFixture)
    {
        _postgres = postgresFixture;

        var factory = new ApiFactory(postgresFixture);

        _client = factory.CreateClient(new WebApplicationFactoryClientOptions
        {
            AllowAutoRedirect = false
        });
    }

    [Fact]
    public async Task CreateShortUrl_WithValidUrl_ReturnsOk()
    {
        var request = new
        {
            originalUrl = "https://www.example.com"
        };

        var response = await _client.PostAsJsonAsync(
            "/api/urls",
            request);

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
    }

    [Fact]
    public async Task CreateAndRedirectUrl_WorksEndToEnd()
    {
        // Create short URL
        var request = new
        {
            originalUrl = "https://www.example.com"
        };

        var createResponse = await _client.PostAsJsonAsync(
            "/api/urls",
            request);

        Assert.Equal(HttpStatusCode.Created, createResponse.StatusCode);

        var createResult =
            await createResponse.Content.ReadFromJsonAsync<JsonElement>();

        var shortCode = createResult
            .GetProperty("shortCode")
            .GetString();

        Assert.NotNull(shortCode);

        // Follow the short URL without automatically following the redirect
        var redirectResponse = await _client.GetAsync(
            $"/{shortCode}");

        Assert.Equal(
            HttpStatusCode.Redirect,
            redirectResponse.StatusCode);

        Assert.Equal(
            "https://www.example.com/",
            redirectResponse.Headers.Location?.ToString());

        // Verify click count in PostgreSQL
        var options = new DbContextOptionsBuilder<UrlShortenerDbContext>()
            .UseNpgsql(_postgres.ConnectionString)
            .Options;

        await using var context = new UrlShortenerDbContext(options);

        var savedUrl = await context.Urls
            .SingleAsync(x => x.ShortCode == shortCode);

        Assert.Equal(1, savedUrl.ClickCount);
    }

    [Fact]
    public async Task ShortenUrl_WithMissingUrl_ReturnsBadRequest()
    {
        var request = new
        {
            OriginalUrl = ""
        };

        var response = await _client.PostAsJsonAsync("/api/urls", request);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task ShortenUrl_WithInvalidUrl_ReturnsBadRequest()
    {
        var request = new
        {
            OriginalUrl = "not-a-url"
        };

        var response = await _client.PostAsJsonAsync("/api/urls", request);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task ShortenUrl_WithUnsupportedScheme_ReturnsBadRequest()
    {
        var request = new
        {
            originalUrl = "ftp://example.com/file.txt"
        };

        var response = await _client.PostAsJsonAsync(
            "/api/urls",
            request);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task ShortenUrl_WithJavascriptUrl_ReturnsBadRequest()
    {
        var request = new
        {
            originalUrl = "javascript:alert(1)"
        };

        var response = await _client.PostAsJsonAsync(
            "/api/urls",
            request);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task ShortenUrl_WithUrlExceedingMaxLength_ReturnsBadRequest()
    {
        var longUrl = $"https://example.com/{new string('a', 2048)}";

        var request = new
        {
            originalUrl = longUrl
        };

        var response = await _client.PostAsJsonAsync(
            "/api/urls",
            request);

        Assert.Equal(
            HttpStatusCode.BadRequest,
            response.StatusCode);
    }

    [Fact]
    public async Task CreateShortUrl_WhenRateLimitExceeded_ReturnsTooManyRequests()
    {
        for (var i = 0; i < 5; i++)
        {
            var request = new
            {
                originalUrl = $"https://example.com/rate-limit/{Guid.NewGuid()}"
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
            originalUrl = $"https://example.com/rate-limit/{Guid.NewGuid()}"
        };

        var limitedResponse = await _client.PostAsJsonAsync(
            "/api/urls",
            limitedRequest);

        Assert.Equal(
            HttpStatusCode.TooManyRequests,
            limitedResponse.StatusCode);
    }
    
    [Fact]
    public async Task RedirectToUrl_WhenRateLimitExceeded_ReturnsTooManyRequests()
    {
        var request = new
        {
            originalUrl = $"https://example.com/rate-limit/{Guid.NewGuid()}"
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

        Assert.Equal(
            HttpStatusCode.TooManyRequests,
            limitedResponse.StatusCode);
    }
}