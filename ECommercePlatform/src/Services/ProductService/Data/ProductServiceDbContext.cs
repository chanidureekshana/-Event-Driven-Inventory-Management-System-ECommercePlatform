using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using ProductService.Models;

namespace ProductService.Data
{
    public class ProductServiceDbContext:DbContext
    {
        public ProductServiceDbContext(DbContextOptions<ProductServiceDbContext> options):base(options)
        {

        }
        public DbSet<ProductModel> Products { get;set; }
    }
}