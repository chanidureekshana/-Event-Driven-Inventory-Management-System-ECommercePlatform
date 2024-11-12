using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Components.Web;
using ProductService.Data.Repositories;
using ProductService.Models;

namespace ProductService.Services
{
    public class ProductService:IProductService
    {
        private readonly IProductRepository _productRepository;
        public ProductService(IProductRepository productRepository)
        {
            _productRepository = productRepository;
        }

        public async Task AddProductAsync(ProductModel product)
        {
            if(product.Quantity < 0 )
            {
                throw new ArgumentException("Quantity Cannot be Negative");
            }
            await _productRepository.AddPostAsync(product);
        }

        public async Task DeleteProductAsync(int productId)
        {
            var ex = await _productRepository.GetProductByIdAsync(productId);
            if(ex == null)
            {
                throw new KeyNotFoundException("Product not found");
            }
            await _productRepository.DeleteProductAsync(productId);
        }

        public async Task<IEnumerable<ProductModel>> GetAllProductAsync()
        {
            return await _productRepository.GetAllProductsAsync();
        }

        public async Task<ProductModel> GetProductByIdAsync(int productId)
        {
            var product = await _productRepository.GetProductByIdAsync(productId);
            if(productId == null)
            {
                throw new KeyNotFoundException("Product is notFound");
            }
            return product;
        }

        public async Task UpdateProductAsync(ProductModel product)
        {
            var ex = await _productRepository.GetProductByIdAsync(product.ProductId);
            if(ex== null)
            {
                throw new KeyNotFoundException("Product is not found.");
            }
            await _productRepository.UpdateProductAsync(product);
        }
    }
}