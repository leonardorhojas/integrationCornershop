import os
## Data Processing Libraries
import numpy as np
import pandas as pd

# ORM Libraries
from sqlalchemy.orm import sessionmaker
from models import BranchProduct, Product
from database_setup import engine


PROJECT_DIR = '/home/rafaelleonardo/Python_projects/scrapyCornershop'
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
ASSETS_DIR = os.path.join(PROJECT_DIR, "assets")
PRODUCTS_PATH = os.path.join(ASSETS_DIR, "PRODUCTS.csv")
PRICES_STOCK_PATH = os.path.join(ASSETS_DIR, "PRICES-STOCK.csv")


def process_csv_files():
    products_df = pd.read_csv(filepath_or_buffer=PRODUCTS_PATH, sep="|",)
    prices_stock_df = pd.read_csv(filepath_or_buffer=PRICES_STOCK_PATH, sep="|",)


    prices_stock_df = prices_stock_df[(prices_stock_df.BRANCH == 'MM') | (prices_stock_df.BRANCH == 'RHSM')]
    products_df['DESCRIPTION'] = products_df['DESCRIPTION'].str.replace('<[^<]+?>', '')
    products_df['CATEGORY'] = products_df['CATEGORY'].str.lower() + '|' + products_df['SUB_CATEGORY'].str.lower() 
	products_df = products_df[products_df.SKU.isin(prices_stock_df.SKU)]

    prices_stock_df = prices_stock_df[(prices_stock_df['STOCK'] > 0)]

    

    del products_df['BUY_UNIT']
	del products_df['DESCRIPTION_STATUS']
	del products_df['ORGANIC_ITEM']
	del products_df['KIRLAND_ITEM']
	del products_df['FINELINE_NUMBER']
	del products_df['SUB_SUB_CATEGORY']
	del products_df['SUB_CATEGORY']

	
    
    # write data to database
    products_df.rename(columns={
        'SKU': 'sku',
        'BARCODES': 'barcodes',
        'DESCRIPTION': 'description',
        'NAME': 'name',
        'IMAGE_URL': 'image_url',
        'CATEGORY': 'category',
        'BRAND': 'brand',
    }, inplace=True)

    prices_stock_df.rename(columns={
        'SKU': 'sku',
        'BRANCH': 'branch',
        'PRICE': 'price',
        'STOCK': 'stock',
    }, inplace=True)



	def package_extraction(item):
        item_listed = item['DESCRIPTION'].split()
        if len(item_listed) == 1:
            return np.nan
        package_extracted = item_listed[-1]
        description_extracted = item['DESCRIPTION'].replace(' ' + word1, '')
        return package_extracted, description_extracted

    products_df['package'] , products_df['DESCRIPTION']= products_df.apply(package_extraction, axis=1)
	products_df['store'] = 'Richart Wholesale Club'




   # Open a Session
    Session = sessionmaker(bind=engine)
    session = Session()

    print('*' * 10+ 'Inserting Products' + '*' * 10 )
    session.bulk_insert_mappings(Product, products_df.to_dict(orient="records"))
    session.commit()
    print('*' * 10+ 'Commited Process' + '*' * 10 )

    products_data= pd.read_sql_table('products', engine)
    prices_stock_df['sku']=prices_stock_df['sku'].astype(str)

    joined_table = pd.merge(prices_stock_df, products_data, left_on='sku', right_on='sku', how='left')
    del joined_table['sku']
    del joined_table['barcodes']
    del joined_table['name']
    del joined_table['description']
    del joined_table['image_url']
    del joined_table['category']
    del joined_table['brand']
    del joined_table['package']
    del joined_table['store']

    joined_table.rename(columns={'id':'product_id'}, inplace=True)
    joined_table = joined_table[joined_table['product_id'].notna()]

    print('*' * 10+ 'Inserting branches' + '*' * 10 )
    session.bulk_insert_mappings(BranchProduct, joined_table.to_dict(orient="records"))

    session.commit()
    session.close()

    print('*' * 10+ 'Commited Process' + '*' * 10 )

  

if __name__ == "__main__":
    process_csv_files()
