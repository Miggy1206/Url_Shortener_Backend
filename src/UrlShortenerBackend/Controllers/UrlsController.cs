using Microsoft.AspNetCore.Mvc;
using System.ComponentModel.DataAnnotations;
using UrlShortenerBackend.Api.Data;
using UrlShortenerBackend.Api.Models;

namespace UrlShortenerBackend.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class UrlsController : ControllerBase
{
    private readonly UrlShortenerDbContext _context;

    public UrlsController(UrlShortenerDbContext context)
    {
        _context = context;
    }

    [HttpPost]
    public async Task<IActionResult> ShortenUrl(ShortenUrlRequest request)
    {
        var shortCode = Guid.NewGuid().ToString("N")[..6];

        var url = new Url
        {
            OriginalUrl = request.OriginalUrl,
            ShortCode = shortCode,
            CreatedAt = DateTime.UtcNow,
            ClickCount = 0
        };

        _context.Urls.Add(url);
        _context.SaveChanges();

        return Ok(new
        {
            shortCode = shortCode,
            shortUrl = $"{Request.Scheme}://{Request.Host}/{shortCode}",
            originalUrl = request.OriginalUrl
        });
    }
}

public record ShortenUrlRequest([Required][Url] string OriginalUrl);