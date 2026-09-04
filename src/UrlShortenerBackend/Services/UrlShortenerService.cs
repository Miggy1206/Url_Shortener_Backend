using Microsoft.EntityFrameworkCore;
using UrlShortenerBackend.Api.Data;
using UrlShortenerBackend.Api.Models;
using Npgsql;

namespace UrlShortenerBackend.Api.Services;

public class UrlShortenerService(
    UrlShortenerDbContext context) : IUrlShortenerService
{
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

                return url;
            }
            catch (DbUpdateException ex) when (
                ex.InnerException is PostgresException pgEx && 
                pgEx.SqlState == PostgresErrorCodes.UniqueViolation)
            {
                context.Entry(url).State = EntityState.Detached;
            }
        }

        throw new InvalidOperationException(
            "Unable to generate a unique short code.");
    }

    public async Task<Url?> RedirectUrlAsync(string shortCode)
    {
        var url = await context.Urls
            .SingleOrDefaultAsync(x => x.ShortCode == shortCode);

        if (url is null)
        {
            return null;
        }

        url.ClickCount++;

        await context.SaveChangesAsync();

        return url;
    }
}