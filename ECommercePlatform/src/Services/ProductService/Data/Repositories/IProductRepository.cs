using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ProductService.Models;

namespace ProductService.Data.Repositories
{
    public interface IProductRepository
    {
        Task<IEnumerable<ProductModel>> GetAllProductsAsync();
        Task<ProductModel> GetProductByIdAsync(int productId);
        Task AddPostAsync( ProductModel product);
        Task UpdateProductAsync(ProductModel product);
        Task DeleteProductAsync(int productId); 
    }
}