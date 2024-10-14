from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
import sqlite3
import pandas as pd

def export_orders_to_csv():
    conn = sqlite3.connect('data/Northwind_small.sqlite')
    query = "SELECT * FROM 'Order'"
    df = pd.read_sql_query(query, conn)
    df.to_csv('output_orders.csv', index=False)
    conn.close()

def join_and_count_for_rio():
    # Read CSV and database
    orders_df = pd.read_csv('output_orders.csv')
    conn = sqlite3.connect('data/Northwind_small.sqlite')
    query = "SELECT * FROM 'OrderDetail'"
    order_details_df = pd.read_sql_query(query, conn)
    conn.close()

    # Como a coluna 'Id' de order_details_df é basicamente a junção de <OrderId>/<ProductId>, e como
    # essa junção não existe em orders_df, precisamos arrumar alguma forma de fazer a concatenação.
    # Isso foi feito jogando a coluna 'Id' original de order_details_df fora, e renomeando 'OrderId'
    # para 'Id', pois estes mesmos dados estão presentes sob o nome de 'Id' em orders_df
    # Agora, com a junção dos dataframes sendo possível, filtramos por Rio de Janeiro.
    order_details_df.drop(columns='Id', inplace=True)
    order_details_df.rename(columns={'OrderId':'Id'}, inplace=True)

    # Perform join on OrderID
    merged_df = pd.merge(order_details_df, orders_df, on='Id')

    # Filter by ShipCity and calculate total quantity
    rio_quantity = merged_df[merged_df['ShipCity'] == 'Rio de Janeiro']['Quantity'].sum()

    # Write the result to count.txt
    with open('count.txt', 'w') as f:
        f.write(str(rio_quantity))

export_orders_to_csv()
join_and_count_for_rio()
print("EOL")