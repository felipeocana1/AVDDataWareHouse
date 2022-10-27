from extract.extract_countries import extractCountries
from extract.extract_channels import extractChannels
from extract.extract_customers import extractCustomers
from extract.extract_products import extractProducts
from extract.extract_promotions import extractPromotions
from extract.extract_sales import extractSales
from extract.extract_times import extractTimes

import traceback


def extarcts():
    try:
    
        extractChannels()
        
        extractCountries()
        
        extractCustomers()
        
        extractProducts()
        
        extractPromotions()
        
        extractSales()
        
        extractTimes()
        
    except:
        traceback.print_exc()
    finally:
        pass