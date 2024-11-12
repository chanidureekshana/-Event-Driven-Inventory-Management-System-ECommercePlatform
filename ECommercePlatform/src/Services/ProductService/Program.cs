using ProductService.Events;
using ProductService.Events.Handlers;
using ProductService.Kafka;
using ProductService.Services;

var builder = WebApplication.CreateBuilder(args);

// Add Swagger and any other services here
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Configure dependency injection
builder.Services.AddSingleton<IProductService, ProductService.Services.ProductService>(); // Assuming ProductService implements IProductService

// Initialize Kafka producer and consumer
var producer = new ProductProducer("localhost:9092", "inventory-updates-topic");
var eventHandler = new InventoryUpdatedEventHandler(builder.Services.BuildServiceProvider().GetRequiredService<IProductService>());
var consumer = new ProductConsumer("localhost:9092", "inventory-updates-topic", "product-consumer-group", eventHandler);

// Publish an initial event for testing (remove this in production)
await producer.PublishInventoryUpdateAsync(new InventoryUpdatedEvent
{
    ProductId = 1,
    QuantityChange = -5,
    EventType = "Sale"
});

// Start consuming events in a background task
var cts = new CancellationTokenSource();
Task.Run(() => consumer.StartConsuming(cts.Token));

var app = builder.Build();

// Configure middleware for development
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

// Add additional endpoints or controllers as needed
app.MapGet("/inventory-update", () => "Inventory service is running.");

app.Run();

// Define any supporting classes as needed
public record InventoryUpdatedEvent
{
    public int ProductId { get; init; }
    public int QuantityChange { get; init; }
    public string EventType { get; init; } = string.Empty;
}
