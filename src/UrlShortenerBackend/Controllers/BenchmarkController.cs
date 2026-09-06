using Microsoft.AspNetCore.Mvc;
using UrlShortenerBackend.Api.Services;

namespace UrlShortenerBackend.Api.Controllers;

[ApiController]
[Route("benchmark")]
public class BenchmarkController(
    IUrlShortenerService service,
    IHostEnvironment environment) : ControllerBase
{
    [HttpGet("{shortCode}")]
    public async Task<IActionResult> Get(string shortCode)
    {
        if (!environment.IsDevelopment())
        {
            return NotFound();
        }

        var originalUrl = await service.GetOriginalUrlBenchmarkAsync(shortCode);

        if (originalUrl is null)
        {
            return NotFound();
        }

        return Ok(originalUrl);
    }

    [HttpPost("increment/{shortCode}")]
    public async Task<IActionResult> Increment(string shortCode)
    {
        if (!environment.IsDevelopment())
        {
            return NotFound();
        }

        await service.IncrementClickCountBenchmarkAsync(shortCode);

        return Ok();
    }
}