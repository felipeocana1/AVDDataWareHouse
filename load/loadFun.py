
import traceback

from load.load_channels import loadChannels
from load.load_countries import loadCountries
from load.load_customers import loadCustomers
from load.load_products import loadProducts
from load.load_promotions import loadPromotions
from load.load_sales import loadsSales
from load.load_times import loadTimes

def loads(ID):
    try:
        
        loadChannels(ID) 
        loadCountries(ID) 
        loadCustomers(ID)  
        loadProducts(ID)
        loadPromotions(ID) 
        loadTimes(ID)
        loadsSales(ID)   
        
        
        
        
    except:
        traceback.print_exc()
    finally:
        pass
    

    