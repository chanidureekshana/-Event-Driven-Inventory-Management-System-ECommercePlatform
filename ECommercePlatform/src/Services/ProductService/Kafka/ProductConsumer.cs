using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json;
using ProductService.Events;
using ProductService.Events.Handlers;

namespace ProductService.Kafka
{
    public class ProductConsumer
    {
        private readonly IConsumer<string , string> _consumer;
        private readonly InventoryUpdatedEventHandler _eventHandler;
        private readonly string _topicName;

        public ProductConsumer(string bootstrapServers , string topicName , string groupId , InventoryUpdatedEventHandler eventHandler)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = groupId,
                AutoOffsetReset= AutoOffsetReset.Earliest
            };
            _consumer = new ConsumerBuilder<string ,string>(config).Build();
            _topicName = topicName;
            _eventHandler = eventHandler;
        }
        public void StartConsuming(CancellationToken cancellationToken)
        {
            _consumer.Subscribe(_topicName);
            try 
            {
                while(!cancellationToken.IsCancellationRequested)
                {
                    var consumeResult = _consumer.Consume(cancellationToken);
                    if(consumeResult !=null)
                    {
                        var inventoryEvent =JsonConvert.DeserializeObject<InventoryUpdatedEvent>(consumeResult.Message.Value);
                        _eventHandler.HandleAsync(inventoryEvent).Wait();
                    }
                }
            }
            catch(OperationCanceledException)
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