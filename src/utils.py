from collections import Counter
import os
import pandas as pd
import requests
import time

# Corregir nombres de artistas de un dataset, por ejemplo "beatles, the" => "the beatles"
def fix_artists_names(df, csv_route):
    corrections = Counter()

    def fix_artist_name(artist_name):
        if ',' in artist_name:
            # Si hay una coma, invertimos el orden de las palabras
            name_parts = artist_name.split(', ')
            corrected_name = f"{name_parts[1]} {name_parts[0]}"
            
            # Contabilizar la corrección
            corrections[corrected_name] += 1
            return corrected_name
        return artist_name
    
    # Aplicar la función a toda la columna 'artist_name'
    df['artist_name'] = df['artist_name'].apply(fix_artist_name)

    # Verificar los cambios
    print("Corrections Summary:")
    for corrected_name, count in corrections.items():
        print(f"{corrected_name}: {count} times corrected")

    # Verificar los primeros datos del dataframe
    print("\nFirst few rows of corrected data:")
    print(df[['artist_name']].head())

    # Guardar el dataframe modificado
    df.to_csv(csv_route, index=False)

    # Al final, reportar cuántos datos han sido corregidos
    total_corrected = sum(corrections.values())
    print(f"\nTotal number of corrections made: {total_corrected}")

# Obtener recording_id de una canción de MusicBrainz basado en su artista y título
def get_recording_id_by_song(df):
    # Create json url for get recording id
    def get_recording_id(title, artist):
        query = f'recording:"{title}" AND artist:"{artist}"'
        url = f"https://musicbrainz.org/ws/2/recording/?query={query}&fmt=json"

        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                if "recordings" in data and len(data["recordings"]) > 0:
                    return data["recordings"][0]["id"]
            return None
        except Exception as e:
            print(f"Error on search {title} - {artist}: {e}")
            return None
        
    # Parameters
    save_interval = 50
    output_file = "../data/raw/reviewed_songs.csv"
    temp_file = output_file + ".temp"

    # Initialize counters
    total_requests = 0
    rows_processed = 0
    found_count = 0
    not_found_count = 0

    # Process DataFrame in reverse order
    for index in reversed(df.index):
        row = df.loc[index]
        
        # Skip if the row already has a valid Recording ID
        if pd.notna(row["recording_id"]) and row["recording_id"] != "Not found":
            continue

        total_requests += 1
        rows_processed += 1
        print(f"\n[Progress] Request {total_requests}/{len(df)}...")

        # Get Recording ID
        recording_id = get_recording_id(row["song_name"], row["artist_name"])

        # Add the result to the DataFrame
        if recording_id == "Not found" or not recording_id:
            df.at[index, "recording_id"] = "Not found"
            not_found_count += 1
            print(f"  ⚠️ Not Recording ID found for: {row['song_name']} - {row['artist_name']}")
        else:
            df.at[index, "recording_id"] = recording_id
            found_count += 1
            print(f"  ✅ Recording ID found: {recording_id}")

        # Print percentage of "Not found" vs valid IDs
        total_processed_so_far = found_count + not_found_count
        total_rows = len(df)

        print(f"  [Stats] - Not Found: {not_found_count} Found: {found_count}")
        print(f"  [Stats] - Processed: {total_processed_so_far} / {total_rows}")

        # Save progress at intervals
        if rows_processed % save_interval == 0:
            try:
                print(f"Saving progress to temporary file: {temp_file}")
                df.to_csv(temp_file, index=False)
            except Exception as e:
                print(f"Error saving file: {e}")

        # Respect the API rate limit
        time.sleep(1)

    # Final save after processing all rows
    try:
        print(f"Final save to file: {output_file}")
        os.rename(temp_file, output_file)  # Replace temporary file with final file
    except Exception as e:
        print(f"Error during final save: {e}")

# Añadir registros que tienen un recording_id correcto de un dataset A a un dataset B, siempre que no existan ya en este
def add_data_with_recording_id(dataset_a: pd.DataFrame, dataset_b: pd.DataFrame) -> pd.DataFrame:
    # Filtrar dataset_a donde 'recording_id' no sea NaN ni "Not found"
    filtered_a = dataset_a[
        dataset_a['recording_id'].notna() & 
        (dataset_a['recording_id'] != "Not found")
    ]
    
    # Excluir los registros de filtered_a que ya están en dataset_b según 'recording_id'
    unique_records = filtered_a[
        ~filtered_a['recording_id'].isin(dataset_b['recording_id'])
    ]
    
    # Contar los registros que se añadirán
    num_added_records = len(unique_records)
    
    # Añadir los registros únicos al dataset_b
    updated_dataset_b = pd.concat([dataset_b, unique_records], ignore_index=True)
    print(f"Added: {num_added_records}")
    
    return updated_dataset_b 
