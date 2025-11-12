import pandas as pd
import os

def excel_sheets_to_csv(excel_file, output_csv, start_cell='B3'):
    """
    Convierte todas las hojas de un archivo Excel en un único archivo CSV.
    Todas las hojas deben comenzar en la misma celda (por defecto B3).
    
    Args:
        excel_file: Ruta del archivo Excel de entrada
        output_csv: Ruta del archivo CSV de salida
        start_cell: Celda inicial donde comienzan los datos (por defecto 'B3')
    """
    
    # Leer todas las hojas del archivo Excel
    excel_data = pd.ExcelFile(excel_file)
    
    # Lista para almacenar todos los DataFrames
    all_dataframes = []
    
    # Calcular skiprows y usecols desde start_cell
    # Por ejemplo, 'B3' significa: skipear las 2 primeras filas y empezar desde columna B
    col_letter = ''.join(filter(str.isalpha, start_cell))
    row_number = int(''.join(filter(str.isdigit, start_cell)))
    
    # Convertir letra de columna a número (A=0, B=1, C=2, etc.)
    start_col = ord(col_letter.upper()) - ord('A')
    skiprows = row_number - 1  # -1 porque los índices empiezan en 0
    
    print(f"Procesando archivo: {excel_file}")
    print(f"Iniciando desde celda: {start_cell} (fila {row_number}, columna {col_letter})")
    print("-" * 80)
    
    # Iterar sobre cada hoja
    for sheet_name in excel_data.sheet_names:
        print(f"\nProcesando hoja: '{sheet_name}'")
        
        try:
            # Leer la hoja completa primero para determinar dimensiones
            df_full = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)
            
            # Leer desde la celda especificada
            df = pd.read_excel(
                excel_file, 
                sheet_name=sheet_name,
                skiprows=skiprows,
                usecols=lambda x: x >= start_col if isinstance(x, int) else True
            )
            
            # Agregar columna con el nombre de la hoja
            df.insert(0, 'Hoja_Origen', sheet_name)
            
            print(f"  - Dimensiones: {df.shape[0]} filas x {df.shape[1]} columnas")
            print(f"  - Columnas: {list(df.columns)}")
            
            all_dataframes.append(df)
            
        except Exception as e:
            print(f"  - ERROR al procesar hoja '{sheet_name}': {str(e)}")
            continue
    
    # Concatenar todos los DataFrames
    if all_dataframes:
        print("\n" + "=" * 80)
        print("Concatenando todas las hojas...")
        
        # Concatenar con ignore_index para reiniciar el índice
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        
        print(f"Total de filas en archivo combinado: {len(combined_df)}")
        print(f"Total de columnas: {len(combined_df.columns)}")
        
        # Guardar como CSV
        combined_df.to_csv(output_csv, index=False, encoding='utf-8-sig')
        print(f"\n✓ Archivo CSV guardado exitosamente: {output_csv}")
        
        return combined_df
    else:
        print("\n✗ No se pudieron procesar hojas del archivo Excel")
        return None


# Ejemplo de uso
if __name__ == "__main__":
    # Configuración
    archivo_excel = "PROYECTO_DESGLOSE_TORRES.xlsx"  # Cambia esto por el nombre de tu archivo
    archivo_csv_salida = "DATOS_CONSOLIDADOS_TORRES.csv"
    celda_inicio = "B3"  # Todas las hojas empiezan en B3
    
    # Verificar que el archivo existe
    if not os.path.exists(archivo_excel):
        print(f"ERROR: El archivo '{archivo_excel}' no existe.")
        print("Por favor, coloca tu archivo Excel en el mismo directorio que este script")
        print("o proporciona la ruta completa al archivo.")
    else:
        # Ejecutar conversión
        df_resultado = excel_sheets_to_csv(archivo_excel, archivo_csv_salida, celda_inicio)
        
        # Mostrar vista previa
        if df_resultado is not None:
            print("\n" + "=" * 80)
            print("Vista previa de las primeras filas:")
            print(df_resultado.head(10))
            print("\nVista previa de las últimas filas:")
            print(df_resultado.tail(10))
