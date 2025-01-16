import pandas as pd

# Ruta del archivo de entrada y salida
input_file = r"C:\Users\solan\Downloads\get_data_from_songs\data\raw\more_songs_with_data_3.csv"
output_file_404 = r"C:\Users\solan\Downloads\get_data_from_songs\data\processed\404_part_3.csv"
output_file_processed = r"C:\Users\solan\Downloads\get_data_from_songs\data\processed\processed_part_3.csv"

# Cargar el archivo CSV en un DataFrame
data = pd.read_csv(input_file)

# Filtrar filas con error == 404.0
error_404 = data[data['error'] == 404.0]
processed_data = data[data['error'] != 404.0]

# Guardar los resultados en archivos separados
error_404.to_csv(output_file_404, index=False)
processed_data.to_csv(output_file_processed, index=False)

print("Archivo separado correctamente:")
print(f"Filas con error 404 guardadas en: {output_file_404}")
print(f"Filas procesadas guardadas en: {output_file_processed}")
