using Microsoft.EntityFrameworkCore;
using Npgsql;
using StackExchange.Redis;
using UrlShortenerBackend.Api.Data;
using UrlShortenerBackend.Api.Models;

namespace UrlShortenerBackend.Api.Services;

public class UrlShortenerService(
    UrlShortenerDbContext context,
    IConnectionMultiplexer redis,
    ILogger<UrlShortenerService> logger) : IUrlShortenerService
{
    private readonly IDatabase _cache = redis.GetDatabase();

    public async Task<Url> CreateShortUrlAsync(string originalUrl)
    {
        for (var attempt = 0; attempt < 3; attempt++)
        {
            var shortCode = Guid.NewGuid().ToString("N")[..6];

            if (await context.Urls.AnyAsync(x => x.ShortCode == shortCode))
            {
                continue;
            }

            var url = new Url
            {
                OriginalUrl = originalUrl,
                ShortCode = shortCode,
                CreatedAt = DateTime.UtcNow,
                ClickCount = 0
            };

            context.Urls.Add(url);

            try
            {
                await context.SaveChangesAsync();

                logger.LogInformation(
                    "Short URL created with short code {ShortCode}",
                    shortCode);

                return url;
            }
            catch (DbUpdateException ex) when (
                ex.InnerException is PostgresException postgresException &&
                postgresException.SqlState == PostgresErrorCodes.UniqueViolation)
            {
                logger.LogWarning(
                    "Short-code collision detected for {ShortCode} on attempt {Attempt}",
                    shortCode,
                    attempt + 1);
                context.Entry(url).State = EntityState.Detached;
            }
        }

        throw new InvalidOperationException(
            "Unable to generate a unique short code.");
    }

    public async Task<string?> RedirectUrlAsync(string shortCode)
    {
        var cacheKey = $"url:{shortCode}";

        try
        {
            var cachedOriginalUrl = await _cache.StringGetAsync(cacheKey);

            if (cachedOriginalUrl.HasValue)
            {
                await IncrementClickCountAsync(shortCode);

                logger.LogDebug(
                    "Redis cache hit for short code {ShortCode}",
                    shortCode);

                return cachedOriginalUrl!;
            }

            logger.LogDebug(
                "Redis cache miss for short code {ShortCode}",
                shortCode);
        }
        catch (RedisException ex)
        {
            logger.LogWarning(
                ex,
                "Redis read failed for short code {ShortCode}. Falling back to PostgreSQL.",
                shortCode);
        }

        var url = await context.Urls
            .SingleOrDefaultAsync(x => x.ShortCode == shortCode);

        if (url is null)
        {
            logger.LogInformation(
                "Short code {ShortCode} not found in PostgreSQL.",
                shortCode);
    
            return null;
        }

        url.ClickCount++;

        await context.SaveChangesAsync();

        try
        {
            await _cache.StringSetAsync(
                cacheKey,
                url.OriginalUrl);
        }
        catch (RedisException ex)
        {
            logger.LogWarning(
                ex,
                "Redis write failed for short code {ShortCode}. Continuing without cache.",
                shortCode);
        }

        logger.LogInformation(
            "Redirect completed for short code {ShortCode}",
            shortCode);

        return url.OriginalUrl;
    }

    private async Task IncrementClickCountAsync(string shortCode)
    {
        var url = await context.Urls
            .SingleOrDefaultAsync(x => x.ShortCode == shortCode);

        if (url is null)
        {
            return;
        }

        url.ClickCount++;

        await context.SaveChangesAsync();
    }
}