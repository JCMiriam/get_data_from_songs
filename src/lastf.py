import pandas as pd
import requests
import os
from dotenv import load_dotenv
import time

# Cargar variables de entorno
load_dotenv()
API_KEY = os.getenv('LASTFM_API_KEY')  # Clave de API desde el archivo .env
SECRET = os.getenv('LASTFM_SECRET')    # Secreto desde el archivo .env
BASE_URL = 'http://ws.audioscrobbler.com/2.0/'

# Cargar el dataset
input_file = r'C:\Users\solan\Downloads\clasificador-letras\data\cleaned_combined_with_language.csv'
output_file = r'C:\Users\solan\Downloads\clasificador-letras\data\full_dataset_with_genres_and_tags.csv'
error_log_file = r'C:\Users\solan\Downloads\clasificador-letras\data\error_log.txt'

# Cargar datos existentes si ya se ha procesado parcialmente
if os.path.exists(output_file):
    processed_df = pd.read_csv(output_file)
    processed_ids = set(processed_df.index)
else:
    processed_df = pd.DataFrame()
    processed_ids = set()

# Cargar el dataset original
df = pd.read_csv(input_file)

# Función para registrar errores
def log_error(message):
    with open(error_log_file, 'a') as f:
        f.write(message + '\n')

# Función para obtener géneros desde Last.fm con manejo de errores
def get_genres(artist):
    params = {
        'method': 'artist.getinfo',
        'artist': artist,
        'api_key': API_KEY,
        'format': 'json'
    }
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()  # Verificar errores HTTP
        data = response.json()
        if 'artist' in data and 'tags' in data['artist']:
            genres = [tag['name'] for tag in data['artist']['tags']['tag']]
            return ', '.join(genres) if genres else 'Unknown'
    except (requests.exceptions.RequestException, ValueError) as e:
        error_message = f"Error al obtener géneros para el artista '{artist}': {e}"
        print(error_message)
        log_error(error_message)
    return 'Unknown'

# Función para obtener tags desde Last.fm para canciones con manejo de errores
def get_track_tags(artist, track):
    params = {
        'method': 'track.getinfo',
        'artist': artist,
        'track': track,
        'api_key': API_KEY,
        'format': 'json'
    }
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()  # Verificar errores HTTP
        data = response.json()
        if 'track' in data and 'toptags' in data['track']:
            tags = [tag['name'] for tag in data['track']['toptags']['tag']]
            return ', '.join(tags) if tags else 'Unknown'
    except (requests.exceptions.RequestException, ValueError) as e:
        error_message = f"Error al obtener tags para la canción '{track}' de '{artist}': {e}"
        print(error_message)
        log_error(error_message)
    return 'Unknown'

# Añadir columnas de géneros de artistas y tags de canciones
def process_row(index, row):
    artist = row['ARTIST_NAME']
    track = row['SONG_NAME']
    genres = get_genres(artist)
    tags = get_track_tags(artist, track)
    return genres, tags

print("Procesando el dataset...")
processed_count = 0
start_time = time.time()

for index, row in df.iterrows():
    if index in processed_ids:
        continue

    genres, tags = process_row(index, row)
    row['genres'] = genres
    row['tags'] = tags
    processed_df = pd.concat([processed_df, pd.DataFrame([row])], ignore_index=True)
    processed_count += 1

    # Imprimir progreso de cada canción procesada
    print(f"Procesado: {row['ARTIST_NAME']} - {row['SONG_NAME']}")

    # Guardar cada 1000 registros procesados
    if processed_count % 1000 == 0:
        processed_df.to_csv(output_file, index=False)
        elapsed_time = time.time() - start_time
        print(f"Procesados {processed_count} registros en {elapsed_time:.2f} segundos.")

# Guardar los datos finales
processed_df.to_csv(output_file, index=False)
print(f"Procesamiento completo. Archivo guardado en {output_file}")
