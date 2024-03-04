from newsapi import NewsApiClient as Nw
import psycopg2
from datetime import datetime
import pandas as pd 

newsapi = Nw(api_key='d0b09d44ab00472b8b2b4ecb8b016e13')

# Conexión a Redshift
    
url = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
data_base = "data-engineer-database"
user = "valecaldirolibalenzuela_coderhouse"
pwd = "m410M6VNNk"
try:
    conn = psycopg2.connect(
        host = url,
        dbname = data_base,
        user = user,
        password = pwd,
        port='5439'
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
            Fecha_carga date,
            primary key(Titulo, Fuente)
        )
    """)
    conn.commit()

    cur.execute("TRUNCATE TABLE coder_noticias")
    count = cur.rowcount

# Trabajo con los datos 
    
resultado = newsapi.get_everything(q='Argentina',
                       sort_by='publishedAt',
                        language='es',
                        page_size=100)
datos = {'Titulo': [], 'Fuente_id': [],'Fuente': [], 'Autor': [], 'URL': [],'Fecha_publicacion': []}
for noticia in resultado['articles']:

    datos['Titulo'].append(noticia['title'])
    datos['Fuente_id'].append(noticia['source']['id'])
    datos['Fuente'].append(noticia['source']['name'])
    datos['Autor'].append(noticia['author'])
    datos['URL'].append(noticia['url'])
    datos['Fecha_publicacion'].append(noticia['publishedAt'])

df = pd.DataFrame(datos)
df.drop_duplicates(subset=['Fuente', 'Autor'], keep='first', inplace=True)
df['Fuente_id'].fillna('Sin datos', inplace=True)
df.loc[df['Fuente_id'] == '', 'Fuente_id'] = 'Sin datos'
print(df)