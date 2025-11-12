import sqlite3
import pandas as pd
import os

# 1. Definir los nombres de los archivos y la base de datos
archivo_entrada = 'DATOS_CONSOLIDADOS_TORRES.csv'
nombre_db = 'desglose_torres.db'
nombre_tabla = 'piezas'

print(f"✅ PASO 2: Iniciando la carga de datos a la base de datos SQLite.")

try:
    # 2. Detectar el delimitador correcto del CSV
    print("🔍 Detectando delimitador del archivo CSV...")
    
    # Leer las primeras líneas para detectar el delimitador
    with open(archivo_entrada, 'r', encoding='utf-8-sig') as f:
        primera_linea = f.readline()
        print(f"   Primera línea del archivo:\n   {primera_linea[:200]}...")
    
    # Detectar delimitador (puede ser ',' o ';')
    delimitador = ','
    if primera_linea.count(';') > primera_linea.count(','):
        delimitador = ';'
    
    print(f"   ✅ Delimitador detectado: '{delimitador}'")
    
    # 3. Leer el archivo CSV con múltiples intentos de codificación
    print("\n🔍 Intentando leer el archivo CSV...")
    
    df = None
    codificaciones = ['utf-8-sig', 'utf-8', 'latin-1', 'iso-8859-1', 'cp1252', 'windows-1252']
    
    for encoding in codificaciones:
        try:
            print(f"   Probando codificación: {encoding}")
            df = pd.read_csv(
                archivo_entrada, 
                sep=delimitador,  # Usar el delimitador detectado
                encoding=encoding, 
                low_memory=False,
                skipinitialspace=True  # Eliminar espacios después del delimitador
            )
            print(f"✅ Archivo leído exitosamente con codificación: {encoding}")
            break
        except UnicodeDecodeError:
            print(f"   ❌ Falló con {encoding}, intentando siguiente...")
            continue
        except Exception as e:
            print(f"   ⚠️ Error con {encoding}: {e}")
            continue
    
    if df is None:
        raise Exception("No se pudo leer el archivo con ninguna codificación estándar. Verifica el archivo CSV.")
    
    print(f"   -> Datos cargados: {df.shape[0]} filas y {df.shape[1]} columnas")
    
    # Verificar que se leyeron múltiples columnas
    if df.shape[1] == 1:
        print("\n⚠️ ADVERTENCIA: Solo se detectó 1 columna.")
        print("   Intentando con delimitadores alternativos...")
        
        # Intentar con otros delimitadores
        for delim in [',', ';', '\t', '|']:
            try:
                df_test = pd.read_csv(archivo_entrada, sep=delim, encoding='utf-8-sig', low_memory=False, nrows=5)
                if df_test.shape[1] > 1:
                    print(f"   ✅ Encontrado delimitador correcto: '{delim}'")
                    df = pd.read_csv(archivo_entrada, sep=delim, encoding='utf-8-sig', low_memory=False)
                    print(f"   -> Datos recargados: {df.shape[0]} filas y {df.shape[1]} columnas")
                    break
            except:
                continue
    
    # 4. Limpieza de nombres de columnas (ESENCIAL para la búsqueda en la app web)
    print("\n🧹 Limpiando nombres de columnas...")
    print("   Columnas originales:")
    for i, col in enumerate(df.columns, 1):
        print(f"      {i}. [{col}]")
    
    # Convertir a mayúsculas, quitar espacios y reemplazar caracteres especiales por guion bajo
    df.columns = df.columns.str.strip().str.upper()
    df.columns = df.columns.str.replace(' ', '_', regex=False)
    df.columns = df.columns.str.replace('(', '', regex=False)
    df.columns = df.columns.str.replace(')', '', regex=False)
    df.columns = df.columns.str.replace('.', '', regex=False)
    df.columns = df.columns.str.replace('Á', 'A', regex=False)
    df.columns = df.columns.str.replace('É', 'E', regex=False)
    df.columns = df.columns.str.replace('Í', 'I', regex=False)
    df.columns = df.columns.str.replace('Ó', 'O', regex=False)
    df.columns = df.columns.str.replace('Ú', 'U', regex=False)
    df.columns = df.columns.str.replace('Ñ', 'N', regex=False)
    
    # CRÍTICO: Manejar columnas duplicadas
    print("\n🔍 Verificando columnas duplicadas...")
    columnas_duplicadas = df.columns[df.columns.duplicated()].tolist()
    
    if columnas_duplicadas:
        print(f"   ⚠️ Se encontraron columnas duplicadas: {set(columnas_duplicadas)}")
        print("   ✅ Renombrando columnas duplicadas...")
        
        # Crear nombres únicos para columnas duplicadas
        nuevas_columnas = []
        contador = {}
        
        for col in df.columns:
            if col in contador:
                contador[col] += 1
                nuevo_nombre = f"{col}_{contador[col]}"
                nuevas_columnas.append(nuevo_nombre)
                print(f"      '{col}' → '{nuevo_nombre}'")
            else:
                contador[col] = 0
                nuevas_columnas.append(col)
        
        df.columns = nuevas_columnas
    else:
        print("   ✅ No se encontraron columnas duplicadas")
    
    print("\n   Columnas finales:")
    for i, col in enumerate(df.columns, 1):
        print(f"      {i}. {col}")
    
    # 5. Conexión a la base de datos SQLite
    print(f"\n💾 Conectando a la base de datos: {nombre_db}")
    conn = sqlite3.connect(nombre_db)
    cursor = conn.cursor()
    print(f"✅ Conectado exitosamente")
    
    # Mostrar vista previa de los datos
    print(f"\n📋 Vista previa de los datos (primeras 3 filas):")
    print(df.head(3).to_string())
    
    # 6. Cargar el DataFrame a la tabla de SQLite
    print(f"\n⏳ Cargando {df.shape[0]} registros a la tabla '{nombre_tabla}'...")
    df.to_sql(nombre_tabla, conn, if_exists='replace', index=False)
    print(f"✅ Datos cargados exitosamente")
    
    # 7. Crear índices para optimizar la velocidad de búsqueda
    print(f"\n🔧 Creando índices para mejorar el rendimiento...")
    
    # Verificar qué columnas existen antes de crear índices
    columnas_para_index = []
    columnas_posibles = ['TIPO', 'FABRICANTE', 'PARTE_DIVISION', 'CABEZA', 'CUERPO', 'TRAMO']
    
    for col in columnas_posibles:
        if col in df.columns:
            columnas_para_index.append(col)
    
    if columnas_para_index:
        columnas_str = ', '.join(columnas_para_index)
        index_sql = f"CREATE INDEX IF NOT EXISTS idx_busqueda ON {nombre_tabla} ({columnas_str});"
        cursor.execute(index_sql)
        conn.commit()
        print(f"✅ Índices creados para: {columnas_str}")
    
    # 8. Verificación final
    print(f"\n🔍 Verificando la carga...")
    cursor.execute(f"SELECT COUNT(*) FROM {nombre_tabla}")
    total_filas = cursor.fetchone()[0]
    print(f"✅ Total de registros en la base de datos: {total_filas}")
    
    # Verificar estructura de la tabla
    cursor.execute(f"PRAGMA table_info({nombre_tabla})")
    columnas_db = cursor.fetchall()
    print(f"\n✅ Estructura de la tabla en SQLite ({len(columnas_db)} columnas):")
    for col in columnas_db[:10]:  # Mostrar primeras 10 columnas
        print(f"   - {col[1]} ({col[2]})")
    if len(columnas_db) > 10:
        print(f"   ... y {len(columnas_db) - 10} columnas más")
    
    # Verificar columnas importantes
    print(f"\n✅ Verificando columnas importantes:")
    columnas_importantes = [
        'FABRICANTE', 'TIPO', 'PARTE_DIVISION', 'CABEZA', 'CUERPO', 'TRAMO',
        'ID_ITEM', 'POSICION', 'DESCRIPCION', 'TEXTO_BREVE_DEL_MATERIAL',
        'LONG_1', 'LONG_2_PRINCIPAL', 'CANTIDAD_X_TORRE', 'PESO_UNITARIO',
        'PLANO', 'MOD_PLANO'
    ]
    
    for col in columnas_importantes:
        if col in df.columns:
            print(f"   ✅ {col}: Presente")
        else:
            print(f"   ⚠️ {col}: NO ENCONTRADA")
    
    conn.close()
    
    print(f"\n" + "="*60)
    print(f"✨ ¡PROCESO COMPLETADO CON ÉXITO!")
    print(f"="*60)
    print(f"📁 Base de datos: {nombre_db}")
    print(f"📊 Tabla: {nombre_tabla}")
    print(f"📈 Registros: {total_filas}")
    print(f"📋 Columnas: {len(df.columns)}")
    print(f"\n🚀 Ya puedes ejecutar: Servidor.py")
    
except FileNotFoundError:
    print(f"❌ Error: El archivo '{archivo_entrada}' no se encontró.")
    print(f"   Asegúrate de que el archivo está en la misma carpeta que este script.")
except Exception as e:
    print(f"\n❌ Ocurrió un error crítico durante la carga a la base de datos:")
    print(f"   {type(e).__name__}: {e}")
    import traceback
    print(f"\n   Detalle del error:")
    traceback.print_exc()
    print(f"\n💡 Sugerencias:")
    print(f"   1. Verifica que el archivo CSV no esté abierto en Excel")
    print(f"   2. Asegúrate de que el CSV tiene el formato correcto")
    print(f"   3. Abre el CSV en un editor de texto para verificar el delimitador")
    print(f"   4. Verifica que la primera línea contiene los encabezados de las columnas")
