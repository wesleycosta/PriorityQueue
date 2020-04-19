using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQ.Services
{
    public class MasterService : IMasterService
    {
        private void Publish()
        {
            var factory = new ConnectionFactory()
            {
                HostName = "localhost",
                Port = 5672,
                UserName = "guest",
                Password = "guest"
            };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "PriorityQueueM",
                                      durable: true,
                                      exclusive: false,
                                      autoDelete: false,
                                      arguments: new Dictionary<string, object>
                                                 {
                                                    { "x-max-priority", 1 },
                                                    { "x-queue-type", "classic" }
                                                 });

                for (int i = 0; i < 10; i++)
                {
                    var priority = i % 2 == 0 ? (byte)0 : (byte)1;
                    var message = $"MSG {i} / PRIORITY: {priority}";
                    var body = Encoding.UTF8.GetBytes(message);
                    var properties = channel.CreateBasicProperties();
                    properties.Priority = priority;
                    channel.BasicPublish("", "PriorityQueueM", properties, body);
                }
            }
        }

        private void Consumer()
        {
            var factory = new ConnectionFactory()
            {
                HostName = "localhost",
                Port = 5672,
                UserName = "guest",
                Password = "guest"
            };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "PriorityQueueM",
                                       durable: true,
                                       exclusive: false,
                                       autoDelete: false,
                                       arguments: new Dictionary<string, object>
                                                  {
                                                    { "x-max-priority", 1 },
                                                    { "x-queue-type", "classic" }
                                                  });

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += Consumer_Received;
                channel.BasicConsume(queue: "PriorityQueueM",
                     autoAck: true,
                     consumer: consumer);

                Console.WriteLine("Aguardando mensagens para processamento");
                Console.WriteLine("Pressione uma tecla para encerrar...");
                Console.ReadKey();
            }
        }

        private void Consumer_Received(
           object sender, BasicDeliverEventArgs e)
        {
            var message = Encoding.UTF8.GetString(e.Body);
            Console.WriteLine(Environment.NewLine +
                "[Nova mensagem recebida] " + message);
        }
    }
}
