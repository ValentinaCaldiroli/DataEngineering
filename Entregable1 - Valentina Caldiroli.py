from newsapi import NewsApiClient
import psycopg2

newsapi = NewsApiClient(api_key='d0b09d44ab00472b8b2b4ecb8b016e13')

# Obtener las principales noticias en español
top_headlines_es = newsapi.get_top_headlines()


# Imprimir los títulos de las noticias
for article in top_headlines_es['articles']:
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
    print("Conectado a Redshift con éxito!")
    
except Exception as e:
    print("No es posible conectar a Redshift")
    print(e)