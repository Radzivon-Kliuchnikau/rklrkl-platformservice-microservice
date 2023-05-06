using System.Text;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using PlatformService.Dtos;
using RabbitMQ.Client;

namespace PlatformService.AsyncDataServices
{
    public class MessageBusClient : IMessageBusClient
    {
        private readonly IConfiguration _configuration;
        private readonly IConnection? _connection;
        private readonly IModel? _channel;

        public MessageBusClient(IConfiguration configuration)
        {
            _configuration = configuration;
            var factory = new ConnectionFactory()
            {
                HostName = _configuration["RabbitMQHost"],
                Port = int.Parse(_configuration["RabbitMQPort"]!)
            };

            try
            {
                _connection = factory.CreateConnection();
                _channel = _connection.CreateModel();

                _channel.ExchangeDeclare(exchange: _configuration["Exchange"], type: ExchangeType.Fanout);

                _connection.ConnectionShutdown += RabbitMQ_ConnectionShutdown;

                Console.WriteLine("---> Connected to Message Bus");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"---> Couldn't connect to Message bus: {ex.Message}");
            }
        }

        public void PublishNewPlatform(PlatformPublishedDto platformPublishDto)
        {
            var message = JsonSerializer.Serialize<PlatformPublishedDto>(platformPublishDto);

            if (_connection!.IsOpen)
            {
                Console.WriteLine("---> RabbitMQ connection open, sending message");
                SendMessage(message);
            }
            else
            {
                Console.WriteLine("RabbitMQ connection is closed, not sending message");
            }
        }

        public void Dispose()
        {
            Console.WriteLine("---> MessageBus Disposed");

            if (_channel!.IsOpen)
            {
                _channel.Close();
                _connection!.Close();
            }
        }

        private void SendMessage(string message)
        {
            var body = Encoding.UTF8.GetBytes(message);

            _channel.BasicPublish(_configuration["Exchange"], routingKey: "", basicProperties: null, body: body);

            Console.WriteLine($"---> We have send {message}");
        }

        private void RabbitMQ_ConnectionShutdown(object? sender, ShutdownEventArgs eventArgs)
        {
            Console.WriteLine("---> RabbitMQ Connection Shutdown");
        }
    }
}