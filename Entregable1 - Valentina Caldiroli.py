from newsapi import NewsApiClient
import psycopg2
from datetime import datetime

newsapi = NewsApiClient(api_key='d0b09d44ab00472b8b2b4ecb8b016e13')

# Obtener las principales noticias en español
top_headlines = newsapi.get_top_headlines()


# Imprimir los títulos de las noticias
for article in top_headlines['articles']:
    print(article['title'])

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
            id VARCHAR(50) primary key,
            Fuente VARCHAR(100),
            Titulo VARCHAR(300),
            Autor VARCHAR(255),
            URL VARCHAR(300),
            Fecha_publicacion date,
            Fecha_carga date
        )
    """)
    conn.commit()

    cur.execute("TRUNCATE TABLE coder_noticias")
    count = cur.rowcount

