import pandas as pd
import json
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions
from datetime import timedelta
import ast

# Configuración de Couchbase
COUCHBASE_HOST = "couchbase://localhost"  # Cambia por tu host
COUCHBASE_USER = "Administrator"  # Cambia por tu usuario
COUCHBASE_PASSWORD = "MRjunior127"  # Cambia por tu contraseña
BUCKET_NAME = "movies-dataset"  # Cambia por tu bucket
SCOPE_NAME = "_default"  # Cambia si usas un scope personalizado
COLLECTION_NAME = "_default"  # Cambia si usas una colección personalizada

# Ruta del archivo CSV
CSV_FILE = "movies_metadata.csv"  # Cambia por la ruta de tu archivo

def parse_json_field(field):
    """Convierte campos JSON string a objetos Python"""
    if pd.isna(field) or field == '':
        return None
    try:
        # Intentar parsear como JSON válido
        return json.loads(field.replace("'", '"'))
    except:
        try:
            # Intentar con ast.literal_eval para formato Python
            return ast.literal_eval(field)
        except:
            return field

def connect_to_couchbase():
    """Establece conexión con Couchbase"""
    auth = PasswordAuthenticator(COUCHBASE_USER, COUCHBASE_PASSWORD)
    options = ClusterOptions(auth)
    options.apply_profile("wan_development")
    
    cluster = Cluster(COUCHBASE_HOST, options)
    cluster.wait_until_ready(timedelta(seconds=5))
    
    bucket = cluster.bucket(BUCKET_NAME)
    scope = bucket.scope(SCOPE_NAME)
    collection = scope.collection(COLLECTION_NAME)
    
    return collection

def load_and_process_csv(file_path):
    """Carga y procesa el archivo CSV"""
    print(f"Cargando archivo CSV: {file_path}")
    
    # Cargar CSV
    df = pd.read_csv(file_path, low_memory=False)
    
    print(f"Total de registros: {len(df)}")
    print(f"Columnas: {list(df.columns)}")
    
    # Campos que contienen JSON
    json_fields = [
        'belongs_to_collection',
        'genres',
        'production_companies',
        'production_countries',
        'spoken_languages'
    ]
    
    # Procesar campos JSON
    for field in json_fields:
        if field in df.columns:
            print(f"Procesando campo JSON: {field}")
            df[field] = df[field].apply(parse_json_field)
    
    # Convertir el campo adult a booleano
    if 'adult' in df.columns:
        df['adult'] = df['adult'].map({'False': False, 'True': True, False: False, True: True})
    
    # Convertir video a booleano
    if 'video' in df.columns:
        df['video'] = df['video'].map({'False': False, 'True': True, False: False, True: True})
    
    # Convertir valores NaN a None
    df = df.where(pd.notnull(df), None)
    
    return df

def insert_to_couchbase(collection, df, batch_size=100):
    """Inserta los datos en Couchbase"""
    total = len(df)
    success_count = 0
    error_count = 0
    
    print(f"\nIniciando inserción de {total} documentos...")
    
    for idx, row in df.iterrows():
        try:
            # Usar el campo 'id' como clave del documento
            doc_id = f"movie_{row['id']}" if 'id' in row else f"movie_{idx}"
            
            # Convertir la fila a diccionario
            document = row.to_dict()
            
            # Insertar en Couchbase
            collection.upsert(doc_id, document)
            success_count += 1
            
            # Mostrar progreso
            if (idx + 1) % batch_size == 0:
                print(f"Procesados: {idx + 1}/{total} ({(idx + 1) / total * 100:.2f}%)")
        
        except Exception as e:
            error_count += 1
            print(f"Error insertando documento {doc_id}: {str(e)}")
            if error_count > 10:  # Detener si hay muchos errores
                print("Demasiados errores. Deteniendo proceso.")
                break
    
    print(f"\n{'='*50}")
    print(f"Proceso completado!")
    print(f"Documentos insertados exitosamente: {success_count}")
    print(f"Errores: {error_count}")
    print(f"{'='*50}")

def main():
    try:
        # Cargar y procesar CSV
        df = load_and_process_csv(CSV_FILE)
        
        # Conectar a Couchbase
        print("\nConectando a Couchbase...")
        collection = connect_to_couchbase()
        print("Conexión exitosa!")
        
        # Insertar datos
        insert_to_couchbase(collection, df)
        
    except Exception as e:
        print(f"Error en el proceso: {str(e)}")
        raise

if __name__ == "__main__":
    main()
