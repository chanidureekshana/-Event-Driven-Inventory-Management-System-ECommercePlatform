using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ProductService.Services;

namespace ProductService.Events.Handlers
{
    public class InventoryUpdatedEventHandler
    {
        private readonly IProductService _productService;
        public InventoryUpdatedEventHandler(IProductService productService)
        {
            _productService = productService;
        }
        public async Task HandleAsync(InventoryUpdatedEvent inventoryUpdatedEvent)
        {
            try 
            {
                var product = await _productService.GetProductByIdAsync(inventoryUpdatedEvent.ProductId);
                if(product == null)
                {
                    Console.WriteLine($"Product With ID {inventoryUpdatedEvent.ProductId} Not Found");
                    return ;
                }
                product.Quantity+=inventoryUpdatedEvent.QuantityChange;
                if(product.Quantity < 0)
                {
                    Console.WriteLine("Inventory update resulted in negative stock. Adjusting to zero.");
                    product.Quantity = 0;
                }
                await _productService.UpdateProductAsync(product);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error handing InventoryUpdatedEvent: {ex.Message}");
            }
        }

    }
}