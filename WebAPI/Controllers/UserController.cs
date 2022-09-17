using Confluent.Kafka;
using Domain.Constant;
using Domain.ViewModel;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Net;
using System.Text.Json;

namespace WebAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class UserController : ControllerBase
    {
        private readonly ILogger<UserController> _logger;

        public UserController(ILogger<UserController> logger)
        {
            _logger = logger;
        }

        [HttpPost]
        public async Task<IActionResult> CreateUser(UserViewModel user)
        {
            try
            {
                // criar usuario no banco

                // depois chamar o microserviço de envio de e-mail para enviar um e-mail dando as boas-vindas ao usuário ↓
                var config = new ProducerConfig
                {
                    BootstrapServers = Configurations.Server,
                    ClientId = Dns.GetHostName(),
                };

                using var producer = new ProducerBuilder<Null, string>(config).Build();

                var result = await producer.ProduceAsync(Configurations.EmailTopic, new Message<Null, string>
                {
                    Value = JsonSerializer.Serialize<EmailUserViewModel>(user)
                });

                return StatusCode(StatusCodes.Status201Created);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Um erro ocorreu ao tentar criar um usuário");

                return StatusCode(StatusCodes.Status500InternalServerError);
            }
        }
    }
}
