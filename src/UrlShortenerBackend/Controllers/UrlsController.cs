using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
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
        for (var attempt = 0; attempt < 3; attempt++)
        {
            var shortCode = Guid.NewGuid().ToString("N")[..6];

            if (await _context.Urls.AnyAsync(x => x.ShortCode == shortCode))
            {
                continue;
            }

            var url = new Url
            {
                OriginalUrl = request.OriginalUrl,
                ShortCode = shortCode,
                CreatedAt = DateTime.UtcNow,
                ClickCount = 0
            };

            _context.Urls.Add(url);

            try
            {
                await _context.SaveChangesAsync();

                return Ok(new
                {
                    shortCode,
                    shortUrl = $"{Request.Scheme}://{Request.Host}/{shortCode}",
                    originalUrl = request.OriginalUrl
                });
            }
            catch (DbUpdateException)
            {
                _context.Entry(url).State = EntityState.Detached;
            }
        }

        return StatusCode(500, "Unable to generate a unique short code.");
    }

    [HttpGet("/{shortCode}")]
    public async Task<IActionResult> RedirectToUrl(string shortCode)
    {
        var url = await _context.Urls.SingleOrDefaultAsync(x => x.ShortCode == shortCode);

        if (url is null)
        {
            return NotFound();
        }

        url.ClickCount++;
        
        await _context.SaveChangesAsync();

        return Redirect(url.OriginalUrl);
    }

}

public record ShortenUrlRequest([Required][Url] string OriginalUrl);