using Microsoft.EntityFrameworkCore;
using ProductService.Data;
using ProductService.Events;
using ProductService.Events.Handlers;
using ProductService.Kafka;
using ProductService.Services;
using  ProductService.Data.Repositories;
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
 

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();



builder.Services.AddDbContext<ProductServiceDbContext>(options=>
options.UseSqlite(builder.Configuration.GetConnectionString("DefaultConnection")));

builder.Services.AddScoped<IProductService, ProductService.Services.ProductService>();
builder.Services.AddScoped<IProductRepository, ProductRepository>();

// builder.Services.AddScoped<IProductService, ProductService.Services.ProductService>(); 

// var producer = new ProductProducer("localhost:9092", "inventory-updates-topic");
// var eventHandler = new InventoryUpdatedEventHandler(IProductService);
// var consumer = new ProductConsumer("localhost:9092", "inventory-updates-topic", "product-consumer-group", eventHandler);

// Publish an event
// await producer.PublishInventoryUpdateAsync(new InventoryUpdatedEvent
// {
//     ProductId = 1,
//     QuantityChange = -5,
//     EventType = "Sale"
// });

// // Start consuming in a background task
// var cts = new CancellationTokenSource();
// Task.Run(() => consumer.StartConsuming(cts.Token));
var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

var summaries = new[]
{
    "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
};
app.MapControllers(); 
app.MapGet("/weatherforecast", () =>
{
    var forecast =  Enumerable.Range(1, 5).Select(index =>
        new WeatherForecast
        (
            DateOnly.FromDateTime(DateTime.Now.AddDays(index)),
            Random.Shared.Next(-20, 55),
            summaries[Random.Shared.Next(summaries.Length)]
        ))
        .ToArray();
    return forecast;
})
.WithName("GetWeatherForecast")
.WithOpenApi();

app.Run();

record WeatherForecast(DateOnly Date, int TemperatureC, string? Summary)
{
    public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);
}