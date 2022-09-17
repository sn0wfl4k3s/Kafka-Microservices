using Confluent.Kafka;
using Domain.Constant;
using Domain.ViewModel;
using System.Text.Json;

namespace EmailWorkerService
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = Configurations.Server,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                GroupId = "topico-teste-group-0",
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();

            consumer.Subscribe(Configurations.EmailTopic);

            _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
            
            while (!stoppingToken.IsCancellationRequested)
            {
                var consumeResult = consumer.Consume(stoppingToken);

                var emailData = JsonSerializer.Deserialize<EmailUserViewModel>(consumeResult.Message.Value);

                if (emailData is null) continue;

                // implementar o envio de e-mail ↓
                _logger.LogInformation("Enviar para: {0}; Mensagem: Olá {1}, boas-vindas.",
                    emailData.Email, emailData.Nome);

                await Task.Delay(100, stoppingToken);
            }

            consumer.Close();
        }
    }
}