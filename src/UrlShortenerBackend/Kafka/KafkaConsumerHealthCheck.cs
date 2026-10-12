using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace UrlShortenerBackend.Api.Kafka;

public sealed class KafkaConsumerHealthCheck(
    KafkaConsumerHealthState state) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        if (!state.IsRunning)
        {
            return Task.FromResult(
                HealthCheckResult.Unhealthy(
                    "Kafka click-event consumer is not running."));
        }

        var data =
            new Dictionary<string, object?>
            {
                ["running"] = true,
                ["consumerLag"] = state.ConsumerLag,
                ["lastSuccessfulProcessAt"] =
                    state.LastSuccessfulProcessAt
            };

        return Task.FromResult(
            HealthCheckResult.Healthy(
                "Kafka click-event consumer is running.",
                data));
    }
}
