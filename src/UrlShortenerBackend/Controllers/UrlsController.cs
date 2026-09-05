using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations;
using UrlShortenerBackend.Api.Data;
using UrlShortenerBackend.Api.Models;
using UrlShortenerBackend.Api.Services;

namespace UrlShortenerBackend.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class UrlsController : ControllerBase
{
    private readonly IUrlShortenerService _urlShortenerService;

    public UrlsController(IUrlShortenerService urlShortenerService)
    {
        _urlShortenerService = urlShortenerService;
    }

    [HttpPost]
    public async Task<IActionResult> ShortenUrl(ShortenUrlRequest request)
    {
        var url = await _urlShortenerService
            .CreateShortUrlAsync(request.OriginalUrl);

        return Created(
            $"/{url.ShortCode}",
            new
            {
                shortCode = url.ShortCode,
                shortUrl = $"{Request.Scheme}://{Request.Host}/{url.ShortCode}",
                originalUrl = url.OriginalUrl
            });
    }

    [HttpGet("/{shortCode}")]
    public async Task<IActionResult> RedirectToUrl(string shortCode)
    {
        var originalUrl = await _urlShortenerService
            .RedirectUrlAsync(shortCode);

        if (originalUrl is null)
        {
            return NotFound();
        }

        return Redirect(originalUrl);
    }

}

public record ShortenUrlRequest([Required][HttpUrl] string OriginalUrl);