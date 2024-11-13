using System;
using System.Threading;
using Confluent.Kafka;
using Newtonsoft.Json;
using ProductService.Events;
using ProductService.Events.Handlers;

namespace ProductService.Kafka
{
    public class ProductConsumer : IDisposable
    {
        private readonly IConsumer<string, string> _consumer;
        private readonly InventoryUpdatedEventHandler _eventHandler;
        private readonly string _topicName;

        public ProductConsumer(string bootstrapServers, string topicName, string groupId, InventoryUpdatedEventHandler eventHandler)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            _consumer = new ConsumerBuilder<string, string>(config).Build();
            _topicName = topicName;
            _eventHandler = eventHandler;
        }

        public async Task StartConsuming(CancellationToken cancellationToken)
        {
            _consumer.Subscribe(_topicName);

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var consumeResult = _consumer.Consume(cancellationToken);
                    if (consumeResult != null)
                    {
                        var inventoryEvent = JsonConvert.DeserializeObject<ProductService.Events.InventoryUpdatedEvent>(consumeResult.Message.Value);
                        if (inventoryEvent != null) // Check for null to avoid null reference exception
                        {
                            await _eventHandler.HandleAsync(inventoryEvent);
                        }
                        else
                        {
                            Console.WriteLine("Deserialization resulted in a null event.");
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Consumer operation was canceled.");
            }
            finally
            {
                _consumer.Close();
            }
        }

        public void Dispose()
        {
            _consumer.Dispose();
        }
    }
}
