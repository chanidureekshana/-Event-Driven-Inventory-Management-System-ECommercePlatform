using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Confluent.Kafka;
using ProductService.Events;
using Newtonsoft.Json;

namespace ProductService.Kafka
{
    public class ProductProducer
    {
        private readonly IProducer<string , string > _producer;
        private readonly string _topicName;

        public ProductProducer(string bootstrapServers ,string topicName)
        {
            var config = new ProducerConfig { BootstrapServers = bootstrapServers};
            _producer =new ProducerBuilder<string ,string>(config).Build();
            _topicName = topicName;
        }
        public async Task PublishInventoryUpdateAsync(InventoryUpdatedEvent inventoryUpdatedEvent)
        {
            try 
            {
                var message = new Message<string , string>
                {
                    Key = inventoryUpdatedEvent.ProductId.ToString(),
                    Value = JsonConvert.SerializeObject(inventoryUpdatedEvent)
                };
                var deliveryResult = await _producer.ProduceAsync(_topicName , message);
                Console.WriteLine($"Delivered '{deliveryResult.Value}' to '{deliveryResult.TopicPartitionOffset}' ");
            }
            catch(Exception ex)
            {
                Console.WriteLine($"Error pushing message: {ex.Message}");
            }
        }
        public void Dispose()
        {
            _producer.Dispose();
        }
    }


}
