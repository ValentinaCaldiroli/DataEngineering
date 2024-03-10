from newsapi import NewsApiClient as Nw
import psycopg2
from datetime import datetime
import pandas as pd 
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
            Fecha_carga date,
            primary key(Titulo, Fuente)
        )
    """)
    conn.commit()

fecha = datetime.now().strftime('%Y-%m-%d')
# Trabajo con los datos 
    
resultado = newsapi.get_everything(q='Argentina',
                       sort_by='publishedAt',
                        language='es',
                        from_param= fecha,
                        to = fecha,
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