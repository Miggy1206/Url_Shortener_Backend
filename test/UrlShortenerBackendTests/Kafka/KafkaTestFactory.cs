using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using UrlShortenerBackend.Tests.Integration;

namespace UrlShortenerBackend.Tests.Kafka;

public class KafkaTestFactory(PostgresFixture postgresFixture)
    : WebApplicationFactory<Program>
{
    public string TopicName { get; } =
        $"url-clicked-test-{Guid.NewGuid():N}";

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.ConfigureAppConfiguration((_, config) =>
        {
            config.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ConnectionStrings:DefaultConnection"] =
                    postgresFixture.ConnectionString,

                ["Kafka:BootstrapServers"] =
                    "localhost:9093",

                ["Kafka:ClickTopic"] =
                    TopicName,

                ["Kafka:ConsumerGroup"] =
                    $"click-count-consumer-test-{Guid.NewGuid():N}"
            });
        });
    }
}
