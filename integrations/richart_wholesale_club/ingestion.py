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
	products_df = products_df[products_df.SKU.isin(prices_stock_df.SKU)]

    products_df['CATEGORY'] = products_df['CATEGORY'].str.lower() + '|' + products_df['SUB_CATEGORY'].str.lower() 

    del products_df['BUY_UNIT']
	del products_df['DESCRIPTION_STATUS']
	del products_df['ORGANIC_ITEM']
	del products_df['KIRLAND_ITEM']
	del products_df['FINELINE_NUMBER']
	del products_df['SUB_SUB_CATEGORY']
	del products_df['SUB_CATEGORY']

	products_df['DESCRIPTION'] = products_df['DESCRIPTION'].str.replace('<[^<]+?>', '')
    
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


	products_df['store'] = 'Richart Wholesale Wlub'




if __name__ == "__main__":
    process_csv_files()
