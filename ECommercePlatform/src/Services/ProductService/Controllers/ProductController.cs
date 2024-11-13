using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using ProductService.Models;
using ProductService.Services;

namespace ProductService.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class ProductController:ControllerBase
    {
        private readonly IProductService _productService;
        public ProductController(IProductService productService)
        {
            _productService= productService;
        }
        [HttpGet]
        public async Task<ActionResult<IEnumerable<ProductModel>>> GetAllProducts()
        {
            var products = await _productService.GetAllProductAsync();
            return Ok(products);
        } 
        [HttpGet("{id}")]
        public async Task<IActionResult>GetProductById(int id)
        {
            var product = await _productService.GetProductByIdAsync(id);
            if(product == null)
            {
                return NotFound("Product is Not Found");
            }
            return Ok(product);
        }

        [HttpPost]
        public async Task<IActionResult> CreateProduct([FromBody] ProductModel product)
        {
            if(product ==null)
            {
                return BadRequest("Product is Empty");
            }
            await _productService.AddProductAsync(product);
            // return CreatedAtAction(nameof(GetProductById) , new { id = product.ProductId});
            return Ok(product);
        }
        [HttpPut("{id}")]
        public async Task<ActionResult>UpdateProduct(int id , [FromBody] ProductModel product)
        {
            if(id != product.ProductId)
            {
                return BadRequest("Product id mismatch");
            }
            var ex =  await _productService.GetProductByIdAsync(id);
            if(ex==null)return NotFound("Id NotFound");

            await _productService.UpdateProductAsync(product);
            return NoContent();
        }
        [HttpDelete("{id}")]
        public async Task<IActionResult> DeleteProduct(int id)
        {
            var ex = await _productService.GetProductByIdAsync(id);
            if(ex==null)return NotFound("Product Key is Not Found");
            await _productService.DeleteProductAsync(id);
            return NoContent();
        }
    }
}