using System.ComponentModel.DataAnnotations;

namespace UrlShortenerBackend.Api.Models;

public class HttpUrlAttribute : ValidationAttribute
{
    private const int MaxUrlLength = 2048;

    protected override ValidationResult? IsValid(
        object? value,
        ValidationContext validationContext)
    {
        if (value is not string url ||
            string.IsNullOrWhiteSpace(url))
        {
            return new ValidationResult(
                "A valid URL is required.");
        }

        if (url.Length > MaxUrlLength)
        {
            return new ValidationResult(
                $"URL must not exceed {MaxUrlLength} characters.");
        }

        if (!Uri.TryCreate(url, UriKind.Absolute, out var uri))
        {
            return new ValidationResult(
                "The supplied URL is invalid.");
        }

        if (uri.Scheme != Uri.UriSchemeHttp &&
            uri.Scheme != Uri.UriSchemeHttps)
        {
            return new ValidationResult(
                "Only HTTP and HTTPS URLs are supported.");
        }

        return ValidationResult.Success;
    }
}