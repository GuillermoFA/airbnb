# DEPENDENCIES
from config import DB as DB
from sqlalchemy import *
import psycopg2 as ps
import pandas as pd
import numpy as np

#import plotly.express as px
#import os
#import pathlib

# ETL
def clean_data(file_path, city, currency_conv):
    str_cols = ['name', 'room_type', 'neighbourhood']
    columns = ['id', 'name', 'room_type', 'price', 'minimum_nights',
               'latitude', 'longitude',
               'number_of_reviews', 'reviews_per_month', 'availability_365',
               'neighbourhood']

    try:
        data_df = pd.read_csv(file_path, delimiter=',')

        data_df = data_df[columns]
        data_df[str_cols] = data_df[str_cols].astype('string')

        data_df['reviews_per_month'].fillna(0, inplace=True)

        data_df['name'].replace(r'\n', ' ', regex=True, inplace=True)
        data_df['name'].replace(r'[^\w\s!\"£$%\^&\*()_+=\-\-\[\]}{#'';:@~/\.,<>\?\|]', '', regex=True, inplace=True)
        data_df['name'].replace(r'[^\x00-\x7F]+', '', regex=True, inplace=True)
        data_df['name'].str.strip()
        data_df['name'].fillna('None', inplace=True)
        data_df.replace(to_replace='', value='None', regex=True, inplace=True)
        data_df['price'] = data_df['price'].mul(currency_conv)
        data_df['price'] = np.floor(data_df['price']).astype(int)

        data_df.city = city

        return data_df

    except Exception as e:
        print(f'[CLEAN DATA] Hubo un error al limpiar los datos.\nTipo: {e}')
        return False


def export_file(file_name, data_df):
    file_writer = pd.ExcelWriter('data_exported/' + file_name + '.xlsx', engine='openpyxl')

    try:
        conn = db_connect()  # Nos conectamos a la DB para obtener las FK
        city = data_df.city

        # Creamos los dataframe de cada hoja
        fact_rent_df = pd.DataFrame(columns=['city_id', 'neighbourhood_id', 'room_id', 'profit'])
        cities_df = pd.DataFrame({'id': get_fk('city', city, conn), 'name': [city]})
        nh_df = pd.DataFrame(columns=['id', 'city_id', 'name'])
        room_df = pd.DataFrame(columns=['id', 'nh_id', 'name', 'room_type', 'minimum_nights', 'price',
                                        'number_of_reviews', 'reviews_per_month', 'availability_365', 'latitude',
                                        'longitude'])

        # Llenamos de datos el DataFrame de Fact_Rent
        for index, row in data_df.iterrows():
            profit = int(row['reviews_per_month'] * row['price'] * row['minimum_nights'])
            fact_rent_df.loc[index] = [get_fk('city', city, conn),
                                       get_fk('nh', row['neighbourhood'], conn, get_fk('city', city, conn)), row['id'],
                                       profit]

        # Llenamos de datos el DataFrame de Neighbourhood
        for index, row in data_df.drop_duplicates(subset=['neighbourhood']).iterrows():
            nh_df.loc[index] = [get_fk('nh', row['neighbourhood'], conn, get_fk('city', city, conn)),
                                get_fk('city', city, conn), row['neighbourhood']]
        nh_df = nh_df.reset_index().drop(columns=['index'])

        # Llenamos de datos el DataFrame de Rooms
        for index, row in data_df.iterrows():  # Tabla de rooms
            room_df.loc[index] = [row['id'], get_fk('nh', row['neighbourhood'], conn, get_fk('city', city, conn)),
                                  row['name'], row['room_type'], row['minimum_nights'], row['price'],
                                  row['number_of_reviews'], row['reviews_per_month'],
                                  row['availability_365'], row['latitude'], row['longitude']]

        # Escribimos los DataFrame en un nuevo archivo excel
        fact_rent_df.to_excel(file_writer, sheet_name='Fact_Rent', index=False)  # Tabla de hechos (Hoja de Excel)
        cities_df.to_excel(file_writer, sheet_name='Cities', index=False)  # Tabla Cities (Hoja de Excel)
        nh_df.to_excel(file_writer, sheet_name='Neighbourhoods', index=False)  # Tabla Neighbourhoods (Hoja de Excel)
        room_df.to_excel(file_writer, sheet_name='Rooms', index=False)  # Tabla Rooms (Hoja de Excel)

        file_writer.close()
        conn.close()
        print('[EXPORT FILE] Se exporto el archivo con exito.')
        return True

    except Exception as e:
        print(f'[EXPORT FILE] Hubo un error al exportar el archivo.\nTipo: {e}')
        return False


def export_old_data(data_path, file_name):
    cols = ['id', 'neighbourhood', 'room_type', 'price', 'number_of_reviews']

    try:
        file_writer = pd.ExcelWriter('data_exported/' + file_name + '.xlsx', engine='openpyxl')
        data_df = pd.read_csv(data_path, delimiter=',')

        data_df.rename(columns={'room_id': 'id', 'neighborhood': 'neighbourhood', 'reviews': 'number_of_reviews'},
                       inplace=True)
        data_df['neighbourhood'].replace(to_replace=['á', 'é', 'í', 'ó', 'ú', 'â', 'ã'], value='', regex=True,
                                         inplace=True)

        new_data = data_df[cols]

        new_data.to_excel(file_writer, sheet_name='Old_rooms', index=False)  # Tabla Rooms (Hoja de Excel)

        file_writer.close()

        print('[EXPORT FILE] Se exporto el archivo con exito.')
        return True

    except Exception as e:
        print(f'[EXPORT FILE] Hubo un error al exportar el archivo.\nTipo: {e}')
        return False


# DATABASE
def db_connect():
    try:
        engine = create_engine('postgresql+psycopg2://' + DB.USER + ':' + DB.PASSWORD + '@' + DB.HOST + '/' + DB.DATABASE, echo=True)

    except ps.OperationalError as e:
        raise e

    else:
        print('[DB] Conectado.')
        return engine.connect()


def db_create(conn):
    # Queries de las tablas del modelo ER
    cities_table = ("""CREATE TABLE IF NOT EXISTS CITIES (
                      id      SERIAL,
                      name    varchar(255) NOT NULL UNIQUE,
                      PRIMARY KEY (id)
                      )""")
    nh_table = ("""CREATE TABLE IF NOT EXISTS NEIGHBOURHOODS ( 
                      id            SERIAL,
                      city_id       int4 NOT NULL, 
                      name          varchar(255) NOT NULL,
                      FOREIGN KEY (city_id) REFERENCES CITIES(id) ON DELETE CASCADE,
                      PRIMARY KEY (id)
                      )
                    """)
    room_table = ("""CREATE TABLE IF NOT EXISTS ROOMS (
                      id                    bigint, 
                      nh_id                 int4 NOT NULL,
                      name                  varchar(255),
                      room_type             varchar(255) NOT NULL, 
                      minimum_nights        int4,
                      price                 int4 NOT NULL, 
                      number_of_reviews     int4 NOT NULL,
                      reviews_per_month     int4,
                      availability_365      int4,
                      latitude              float4,
                      longitude             float4,
                      FOREIGN KEY (nh_id) REFERENCES NEIGHBOURHOODS(id) ON DELETE CASCADE,
                      PRIMARY KEY (id)
                      )
                    """)

    try:
        conn.execute(cities_table)  # Creamos la tabla Cities
        conn.execute(nh_table)  # Creamos la tabla Neighbourhoods
        conn.execute(room_table)  # Creamos la tabla Rooms

        print("[DB] Se crearon las tablas base.")

        conn.close()
        return True

    except Exception as e:
        print(f'[DB] Hubo un error al crear las tablas base.\nTipo: {e}')
        return False


def db_append_data(data_df, conn):
    # Queries para insertar datos en la DB
    insert_c_query = ("""INSERT INTO CITIES (name) 
                    VALUES(%s)
                    ON CONFLICT (name) DO NOTHING""")
    insert_nh_query = ("""INSERT INTO NEIGHBOURHOODS (city_id, name) 
                    VALUES(%s, %s)
                    ON CONFLICT (id) DO NOTHING""")
    insert_r_query = ("""INSERT INTO ROOMS (
                    id, nh_id, name, room_type, minimum_nights, price, number_of_reviews, reviews_per_month,
                    availability_365,  latitude, longitude) 
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING""")

    try:
        city = data_df.city

        nh_df = data_df.drop_duplicates(subset=['neighbourhood'])
        nh_df = nh_df.reset_index().drop(columns=['index'])

        conn.execute(insert_c_query, city)  # Insertamos datos en Cities

        for index, row in nh_df.iterrows():  # Insertamos datos en Neighbourhoods
            conn.execute(insert_nh_query,
                         [get_fk('city', city, conn), row['neighbourhood']])

        for index, row in data_df.iterrows():  # Insertamos datos en Rooms
            conn.execute(insert_r_query,
                         [row['id'], get_fk('nh', row['neighbourhood'], conn, get_fk('city', city, conn)), row['name'],
                          row['room_type'], row['minimum_nights'], row['price'],
                          row['number_of_reviews'], row['reviews_per_month'], row['availability_365'],
                          row['latitude'], row['longitude']])

        conn.close()
        return True

    except Exception as e:
        print(f'[DB] Hubo un error insertando datos en la base de datos.\nTipo: {e}')
        return False


# UTIL
def get_fk(table, match, conn, city_id=None):
    if table != 'city' and table != 'nh':
        return False

    try:
        if table == 'city':
            query = ("""SELECT id
                    FROM CITIES
                    WHERE name = %s""")

            fk = conn.execute(query, match).fetchone()
            return fk[0]

        if table == 'nh':
            query = ("""SELECT id
                            FROM NEIGHBOURHOODS
                            WHERE name = %s
                            AND   city_id = %s""")

            fk = conn.execute(query, [match, city_id]).fetchone()
            return fk[0]

    except Exception as e:
        print(f'[FK] Hubo un error al intentar encontrar la clave foranea.\nTipo: {e}')
        return False


#menu()


# MAIN PROBAR EL PROGRAMA (YA LO EJECUTÉ, NO LO EJECUTEN DE NUEVO)
#db_create(db_connect())

#df = clean_data('data/airbnb_lisboa.csv', 'Lisboa', currency_conv=1.04)
#df2 = clean_data('data/airbnb_madrid.csv', 'Madrid', currency_conv=1.04)
#df3 = clean_data('data/airbnb_newyork.csv', 'New York', currency_conv=1.0)
#df4 = clean_data('data/airbnb_buenosaires.csv', 'Buenos Aires', currency_conv=0.006)

#db_append_data(df, db_connect())
#db_append_data(df2, db_connect())
#db_append_data(df3, db_connect())
#db_append_data(df4, db_connect())

#export_file('lisboa_pbi', df)
#export_file('madrid_pbi', df2)
#export_file('newyork_pbi', df3)
#export_file('buenosaires_pbi', df4)
#export_old_data('data/airbnb_lisboa_taller1.csv', 'old_data_lisboa')
