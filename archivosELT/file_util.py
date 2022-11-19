import pandas as pd
import os

def cargar_datos(name):
    df = pd.read_csv("/opt/airflow/data/" + name + ".csv", sep=',', encoding = 'latin1', index_col=False)
    return df

def guardar_datos(df, nombre):
    df.to_csv("/opt/airflow/data/" + nombre + ".csv" , encoding = 'latin1', sep=',', index=False)  
    
    
def procesar_datos():
    
    ## Dimension city
    city = pd.read_csv("http://bigdata-cluster4-01.virtual.uniandes.edu.co:50070/webhdfs/v1/user/monitorbi/datalakeBI/dimension_city.csv?op=OPEN&user.name=cursobi28", sep=',', encoding = 'latin1', index_col=False) # recuerden cambiar XX por el número de su grupo
    city = city.dropna(axis=0, how = 'any')
    guardar_datos(city, "dimension_city")

    ## Dimension Customer
    customer = pd.read_csv("http://bigdata-cluster4-01.virtual.uniandes.edu.co:50070/webhdfs/v1/user/monitorbi/datalakeBI/dimension_customer.csv?op=OPEN&user.name=cursobi28", sep=',', encoding = 'latin1', index_col=False)
    customer = customer.dropna(axis=0, how = 'any')
    customer['Customer'] = customer['Customer'].str.replace("'", ' ')
    guardar_datos(customer, "dimension_customer")
    
    ## Dimension Date
    date = pd.read_csv("http://bigdata-cluster4-01.virtual.uniandes.edu.co:50070/webhdfs/v1/user/monitorbi/datalakeBI/dimension_date.csv?op=OPEN&user.name=cursobi28", sep=',', encoding = 'latin1', index_col=False)
    guardar_datos(date, "dimension_date")

    ## Dimension Employee
    employee = pd.read_csv("http://bigdata-cluster4-01.virtual.uniandes.edu.co:50070/webhdfs/v1/user/monitorbi/datalakeBI/dimension_employee.csv?op=OPEN&user.name=cursobi28", sep=',', encoding = 'latin1', index_col=False)
    employee = employee.dropna(axis=0, how='any')
    guardar_datos(employee, "dimension_employee")

    ## Dimension Stock item
    stock_item = pd.read_csv("http://bigdata-cluster4-01.virtual.uniandes.edu.co:50070/webhdfs/v1/user/monitorbi/datalakeBI/dimension_stock_item.csv?op=OPEN&user.name=cursobi28", sep=',', encoding = 'latin1', index_col=False)
    stock_item.loc[(stock_item['Color'].isna() & (stock_item['Stock_Item'].str.contains('Brown'))), 'Color'] = 'Brown'
    stock_item.loc[(stock_item['Color'].isna() & (stock_item['Stock_Item'].str.contains('Black'))), 'Color'] = 'Black'
    stock_item.loc[(stock_item['Color'].isna() & (stock_item['Stock_Item'].str.contains('White'))), 'Color'] = 'White'
    stock_item.loc[(stock_item['Color'].isna() & (stock_item['Stock_Item'].str.contains('Blue'))), 'Color'] = 'Blue'
    stock_item.loc[(stock_item['Color'].isna() & (stock_item['Stock_Item'].str.contains('Red'))), 'Color'] = 'Red'
    stock_item.loc[(stock_item['Color'].isna() & (stock_item['Stock_Item'].str.contains('Gray'))), 'Color'] = 'Gray'
    stock_item.loc[(stock_item['Color'].isna() & (stock_item['Stock_Item'].str.contains('Green'))), 'Color'] = 'Green'
    stock_item.loc[(stock_item['Color'].isna() & (stock_item['Stock_Item'].str.contains('Pink'))), 'Color'] = 'Pink'
    stock_item.loc[(stock_item['Color'].isna() & (stock_item['Stock_Item'].str.contains('Yellow'))), 'Color'] = 'Yellow'
    index_nan = stock_item[stock_item['Stock_Item_Key'] == 0].index
    stock_item.drop(index_nan, inplace=True)
    stock_item.loc[(stock_item['Recommended_Retail_Price']).str.index(',')==0, 'Recommended_Retail_Price'] = "0" + stock_item['Recommended_Retail_Price']
    stock_item.loc[(stock_item['Typical_Weight_Per_Unit']).str.index(',')==0, 'Typical_Weight_Per_Unit'] = "0" + stock_item['Typical_Weight_Per_Unit']
    stock_item.loc[(stock_item['Unit_Price']).str.index(',')==0, 'Unit_Price'] = "0" + stock_item['Unit_Price']
    stock_item['Recommended_Retail_Price'] = stock_item['Recommended_Retail_Price'].str.replace(',', '.')
    stock_item['Typical_Weight_Per_Unit'] = stock_item['Typical_Weight_Per_Unit'].str.replace(',', '.')
    stock_item['Unit_Price'] = stock_item['Unit_Price'].str.replace(',', '.')
    stock_item['Tax_Rate'] = stock_item['Tax_Rate'].str.replace(',', '.')
    stock_item['Stock_Item'] = stock_item['Stock_Item'].str.replace("'", ' ')
    guardar_datos(stock_item, "dimension_stock_item")
    
    ## Fact Table
    fact_order = pd.read_csv("http://bigdata-cluster4-01.virtual.uniandes.edu.co:50070/webhdfs/v1/user/monitorbi/datalakeBI/fact_order.csv?op=OPEN&user.name=cursobi28", sep=',', encoding = 'latin1', index_col=False)
    
    guardar_datos(fact_order, "fact_order")