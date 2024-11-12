using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ProductService.Events
{
    public class InventoryUpdatedEvent
    {
        public int ProductId { get;set; }
        public int QuantityChange { get;set ;}
        public string EventType { get;set; }
        
    }
}