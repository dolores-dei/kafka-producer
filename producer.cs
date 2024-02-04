using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

public class KafkaProducerService : IDisposable
{

    /// <summary>
    /// Delay after error [ms]
    /// </summary>
    private const int AfterErrorDelay = 500;


    private readonly ILogger<KafkaProducerService> _logger;


    private readonly ILogger<LogMessage> _kafkaLogger;




    private IProducer<string, string> _producer;


    public KafkaProducerService(
        ILogger<KafkaProducerService> logger,
        ILogger<LogMessage> kafkaLogger
    )
    {
        this._logger = logger;
        this._kafkaLogger = kafkaLogger;
    }


    ~KafkaProducerService()
    {
        Dispose(disposing: false);
    }


    public void Dispose()
    {
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }


    protected virtual void Dispose(bool disposing)
    {
        if (!disposing)
        {
            return;
        }

        try
        {
            ResetProducer();

        }
        catch (Exception ex)
        {
            this._kafkaLogger.LogWarning(ex, "Error while resetting Kafka producer: {0}", ex.Message);
        }
    }

    //<T> can be any type that can be serialized to JSON
    public async Task SendMessageAsync<T>(T target, string topicName, CancellationToken cancellationToken = default)
    {
        if (position == null || string.IsNullOrEmpty(topicName))
        {
            return;
        }

        var message = new Message<string, string>
        {
            Value = JsonConvert.SerializeObject(target)
        };

        this.EnsureProducer();

        try
        {
            var deliveryResult = await this._producer.ProduceAsync(topicName, message, cancellationToken);

            this._logger.LogDebug("{0}: Message offset {1} for TargetId {2} / {3}, Timestamp {4}",
                nameof(KafkaProducerService), deliveryResult.Offset.Value, position.TargetId, position.Id, position.Timestamp);

        }
        catch (OperationCanceledException)
        {

        }
        catch (Exception ex)
        {
            this.ResetProducer();
            await Task.Delay(KafkaProducerService.AfterErrorDelay, cancellationToken);

            throw ex;
        }
    }

    private void EnsureProducer()
    {
        if (this._producer != null)
        {
            return;
        }
        var builder = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory()) // Set the base path to your project directory
            .AddJsonFile("appsettings.development.json", optional: true, reloadOnChange: true);
        IConfigurationRoot configuration = builder.Build();

        // Read the Kafka producer configuration from the configuration file
        var kafkaConfig = configuration.GetSection("KafkaProducerConfig").Get<ProducerConfig>();

        this._producer = new ProducerBuilder<string, string>(kafkaConfig)
            .SetLogHandler(ProducerLogHandler)
            .Build();
    }


    private void ResetProducer()
    {
        try
        {
            this._producer?.Dispose();

        }
        finally
        {
            this._producer = null;
        }
    }


    private void ProducerLogHandler(IProducer<string, string> producer, LogMessage logMessage)
    {
        switch (logMessage.Level)
        {
            case SyslogLevel.Warning:
                this._kafkaLogger.LogWarning(logMessage.Message);
                break;

            case SyslogLevel.Emergency:
            case SyslogLevel.Alert:
            case SyslogLevel.Critical:
            case SyslogLevel.Error:
                this._kafkaLogger.LogError(logMessage.Message);
                break;

            case SyslogLevel.Debug:
                this._kafkaLogger.LogDebug(logMessage.Message);
                break;

            default:
                this._kafkaLogger.LogInformation(logMessage.Message);
                break;
        }
    }

}
