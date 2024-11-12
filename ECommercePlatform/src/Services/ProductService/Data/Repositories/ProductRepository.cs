using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using ProductService.Models;

namespace ProductService.Data.Repositories
{
    public class ProductRepository:IProductRepository
    {
        private readonly ProductServiceDbContext _context;
        public ProductRepository(ProductServiceDbContext context)
        {
            _context =context ;
        }

        public async Task AddPostAsync(ProductModel product)
        {
            _context.Add(product);
            await _context.SaveChangesAsync();
        }

        public async Task DeleteProductAsync(int productId)
        {
            var product = _context.Products.FindAsync(productId);
            if(product != null)
            {
                _context.Remove(product);
                await _context.SaveChangesAsync();
            }

        }

        public async Task<IEnumerable<ProductModel>> GetAllProductsAsync()
        {
            return await _context.Products.ToListAsync();
        }

        public async Task<ProductModel> GetProductByIdAsync(int productId)
        {
            var product = await _context.Products.FindAsync(productId);
            return product;
        }

        public async Task UpdateProductAsync(ProductModel product)
        {
            _context.Products.Update(product);
            await _context.SaveChangesAsync();
        }
    }
}