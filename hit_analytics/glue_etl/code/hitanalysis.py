import sys
import re

class hitanalysis:
    
    #defining Spark UDF
    def extract_info(self, product_list):
    
        '''
        This method extracts information from Product_list column:
         * First Splits the value by a ','
         * Then explodes each value above by splitting using ';'

         input    --> (product_list {StringType()})
         output   --> (ArrayType(ArrayType(StringType()))) 
        
        '''
        
        products = []
        for product in product_list.split(','):
            local_list = [x for x in product.split(';')]
            #local_list.append(product)
            products.append(local_list)
        return products
            

    #defining Spark UDF
    def parse_reff(self, referrer):
    
        '''
        This Method Parses 'Search Query' from the refferer column
        input -->   (referrer (StringType()))
        output -->  (Search_domain (StringType))
        
        '''
        
        if 'google' in referrer:
            regex = 'http:\/\/\w+.\S+.\w\/search\?.*?&q=(.*?)&'
        elif 'yahoo' in referrer:
            regex = 'http:\/\/\w+.\S+.\w\/search\?p=(\S+?)&'
        elif 'bing' in referrer:
            regex = 'http:\/\/\w+.\S+.\w\/search\?q=(\S+?)&'
        
        p = re.compile(regex)
        m = p.match(referrer)
        return m.group(1)


