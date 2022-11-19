from utils.file_util import cargar_datos

# city insertion
def insert_query_city(**kwargs):
    
    insert = f"INSERT INTO city (City_Key,City,State_Province,Country,Continent,Sales_Territory,Region,Subregion,Latest_Recorded_Population) VALUES "
    insertQuery = ""
    # Es necesario colocar este try porque airflow comprueba el funcionamiento de las tareas en paralelo y al correr el DAG no existe el archivo dimension_city. Deben colocar try y except en todas las funciones de insert
    try:
        dataframe =cargar_datos(kwargs['csv_path'])
        for index, row in dataframe.iterrows():
            insertQuery += insert + f"({row.City_Key},\'{row.City}\',\'{row.State_Province}\',\'{row.Country}\',\'{row.Continent}\',\'{row.Sales_Territory}\',\'{row.Region}\',\'{row.Subregion}\',{row.Latest_Recorded_Population});\n"
        return insertQuery
    except:
        
        return ""

# customer insertion
def insert_query_customer(**kwargs):
    insert = f"INSERT INTO customer (Bill_To_Customer,Buying_Group,Category,Customer,Customer_Key,Postal_Code,Primary_contact) VALUES "
    insertQuery = ""
    try:
        dataframe = cargar_datos(kwargs['csv_path'])
        for index, row in dataframe.iterrows():
            insertQuery += insert + f"(\'{row.Bill_To_Customer}\',\'{row.Buying_Group}\',\'{row.Category}\',\'{row.Customer}\',{row.Customer_Key},{row.Postal_Code},\'{row.Primary_Contact}\');\n"
        return insertQuery
    except:
        
        return ""

# date insertion
def insert_query_date(**kwargs):
    insert = f"INSERT INTO date_table (Calendar_Month_Number,Calendar_Year,Date_key,Day_Number,Day_val,Fiscal_Month_Number,Fiscal_Year,Month_val,Short_Month) VALUES "
    insertQuery = ""
    try:
        dataframe = cargar_datos(kwargs['csv_path'])
        for index, row in dataframe.iterrows():
            insertQuery += insert + f"({row.Calendar_Month_Number},{row.Calendar_Year},TO_DATE(\'{row.Date_key}\','YYYY-MM-DD'),{row.Day_Number},{row.Day_val},{row.Fiscal_Month_Number},{row.Fiscal_Year},\'{row.Month_val}\',\'{row.Short_Month}\');\n"
        return insertQuery
    except:
        
        return ""

# employee insertion
def insert_query_employee(**kwargs):
    insert = f"INSERT INTO employee (Employee_Key,Employee,Preferred_Name,Is_Salesperson) VALUES "
    insertQuery = ""
    try:
        dataframe = cargar_datos(kwargs['csv_path'])
        for index, row in dataframe.iterrows():
            insertQuery += insert + f"({row.Employee_Key},\'{row.Employee}\',\'{row.Preferred_Name}\',{'FALSE' if not row.Is_Salesperson else 'TRUE'});\n"
        return insertQuery
    except:
        return ""

# stock item insertion
def insert_query_stock(**kwargs):
    insert = f"INSERT INTO stockitem (Stock_Item_Key,Stock_Item,Color,Selling_Package,Buying_Package,Brand,Size_val,Lead_Time_Days,Quantity_Per_Outer,Is_Chiller_Stock,Tax_Rate,Unit_Price,Recommended_Retail_Price,Typical_Weight_Per_Unit) VALUES "
    insertQuery = ""
    try:
        dataframe = cargar_datos(kwargs['csv_path'])
        for index, row in dataframe.iterrows(): 
            insertQuery += insert + "({},\'{}\',{},\'{}\',\'{}\',{},{},{},{},{},{},{},{},{});\n".format(
                row.Stock_Item_Key,
                row.Stock_Item,
                'NULL' if row.Color == 'nan' else f'\'{row.Color}\'',
                row.Selling_Package,
                row.Buying_Package,
                'NULL' if row.Brand == 'nan' else "\'{}\'".format(row.Brand),
                'NULL' if row.Size_val == 'nan' else f'\'{row.Size_val}\'',
                row.Lead_Time_Days,
                row.Quantity_Per_Outer,
                'FALSE' if not row.Is_Chiller_Stock else 'TRUE',
                row.Tax_Rate,
                row.Unit_Price,
                row.Recommended_Retail_Price,
                row.Typical_Weight_Per_Unit
                )
        return insertQuery
    except:
        
        return ""

# fact order insert
def insert_query_fact_order(**kwargs):
    insert = f"INSERT INTO fact_order (Order_key,City_key,Customer_key,Stock_item_key,Order_date_key,Picked_date_key,Salesperson_key,Picker_key,Package,Quantity,Unit_price,Tax_rate,Total_excluding_tax,Tax_amount,Total_including_tax) VALUES "
    insertQuery = ""
    try:
        dataframe = cargar_datos(kwargs['csv_path'])
        for index, row in dataframe.iterrows():
            insertQuery += insert + f"({row.order_key},{row.city_key},{row.customer_key},{row.stock_item_key},TO_DATE(\'{row.order_date_key}\','YYYY-MM-DD'),TO_DATE(\'{row.picked_date_key}\','YYYY-MM-DD'),{row.salesperson_key},{row.picker_key},\'{row.package}\',{row.quantity},{row.unit_price},{row.tax_rate},{row.total_excluding_tax},{row.tax_amount},{row.total_including_tax});\n"
        return insertQuery
    except:
        return ""