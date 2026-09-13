namespace UrlShortenerBackend.Api.Kafka;

public sealed class KafkaConsumerHealthState
{
    private int _isRunning;

    private long _lastSuccessfulProcessTicks;

    private long _consumerLag;

    public bool IsRunning =>
        Volatile.Read(ref _isRunning) == 1;

    public DateTime? LastSuccessfulProcessAt
    {
        get
        {
            var ticks =
                Volatile.Read(
                    ref _lastSuccessfulProcessTicks);

            return ticks == 0
                ? null
                : new DateTime(
                    ticks,
                    DateTimeKind.Utc);
        }
    }

    public long ConsumerLag =>
        Math.Max(
            0,
            Volatile.Read(ref _consumerLag));

    public void MarkStarted()
    {
        Volatile.Write(
            ref _isRunning,
            1);
    }

    public void MarkStopped()
    {
        Volatile.Write(
            ref _isRunning,
            0);

        Interlocked.Exchange(
            ref _consumerLag,
            0);
    }

    public void MarkProcessed()
    {
        Interlocked.Exchange(
            ref _lastSuccessfulProcessTicks,
            DateTime.UtcNow.Ticks);
    }

    public void SetConsumerLag(long lag)
    {
        Interlocked.Exchange(
            ref _consumerLag,
            Math.Max(0, lag));
    }
}
