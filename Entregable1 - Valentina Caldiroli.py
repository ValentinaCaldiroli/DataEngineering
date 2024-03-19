import psycopg2
import pandas as pd

from psycopg2.extras import execute_values
from newsapi import NewsApiClient as Nw
from datetime import datetime, timedelta
from config import NEWAPI_KEY, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_DATABASE

newsapi = Nw(api_key = NEWAPI_KEY)

# Conexión a Redshift

try:
    conn = psycopg2.connect(
        host = DB_HOST,
        dbname = DB_DATABASE,
        user = DB_USER,
        password = DB_PASSWORD,
        port= DB_PORT
    )
    print("Conectado correctamente")
    
except Exception as e:
    print("No es posible la conexión")
    print(e)

# Creación de la tabla

with conn.cursor() as cur:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS coder_noticias
        (
            
            Titulo VARCHAR(300),
            Fuente_id VARCHAR(50),
            Fuente VARCHAR(100),
            Autor VARCHAR(255),
            URL VARCHAR(300),
            Fecha_publicacion date,
            Fecha_carga datetime,
            primary key(Titulo, URL)
        )
    """)
    conn.commit()

fecha = datetime.now()
fecha_ayer = fecha - timedelta(days=1)

# Formatear la fecha como string si es necesario
fecha_ayer_str = fecha_ayer.strftime('%Y-%m-%d')

# Trabajo con los datos 
    
resultado = newsapi.get_everything(q='Argentina',
                       sort_by='publishedAt',
                        language='es',
                        from_param= fecha_ayer_str,
                        to = fecha_ayer_str,
                        page_size=100)
datos = {'Titulo': [], 'Fuente_id': [],'Fuente': [], 'Autor': [], 'URL': [],'Fecha_publicacion': [], 'Fecha_carga' : []}
for noticia in resultado['articles']:

    datos['Titulo'].append(noticia['title'])
    datos['Fuente_id'].append(noticia['source']['id'])
    datos['Fuente'].append(noticia['source']['name'])
    datos['Autor'].append(noticia['author'])
    datos['URL'].append(noticia['url'])
    datos['Fecha_publicacion'].append(noticia['publishedAt'])
    datos['Fecha_carga'].append(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

df = pd.DataFrame(datos)
df.drop_duplicates(subset=['Titulo', 'URL'], keep='first', inplace=True)
df['Fuente_id'].fillna('Sin datos', inplace=True)
df.loc[df['Fuente_id'] == '', 'Fuente_id'] = 'Sin datos'
print(df)

#Insertando los datos en Redsfhift
with conn.cursor() as cur:
    execute_values(
        cur,
        '''
        INSERT INTO coder_noticias 
        VALUES %s
        ''',
        [tuple(row) for row in df.values],
        page_size=len(df)
    )
    conn.commit()


cur.close()
conn.close()