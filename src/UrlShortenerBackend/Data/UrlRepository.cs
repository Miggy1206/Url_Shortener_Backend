using Microsoft.EntityFrameworkCore;

namespace UrlShortenerBackend.Api.Data;

public class UrlRepository(
    UrlShortenerDbContext context) : IUrlRepository
{
    public async Task IncrementClickCountAsync(
        string shortCode,
        CancellationToken cancellationToken = default)
    {
        await context.Urls
            .Where(x => x.ShortCode == shortCode)
            .ExecuteUpdateAsync(
                setters => setters.SetProperty(
                    x => x.ClickCount,
                    x => x.ClickCount + 1),
                cancellationToken);
    }
}