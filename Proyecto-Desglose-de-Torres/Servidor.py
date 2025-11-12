from flask import Flask, request, jsonify, render_template, send_from_directory
from sqlalchemy import create_engine, text
import os
# import re # Sigue sin ser necesario

app = Flask(__name__)

# Configuración de la base de datos
DATABASE_URL = "sqlite:///desglose_torres.db"
engine = create_engine(DATABASE_URL)

@app.route('/')
def index():
    """Sirve la página principal (Buscador: Interfaz.html)."""
    return render_template('Interfaz.html')

@app.route('/calculadora')
def calculadora():
    """Sirve la página de la calculadora de materiales (Calculadora.html)."""
    return render_template('calculadora.html')

# -----------------------------------------------------------
# API para Opciones de Filtro (Filtrado en Cascada)
# -----------------------------------------------------------
@app.route('/api/options', methods=['GET'])
def get_options():
    """
    Ruta: Obtiene los valores únicos para los filtros de la base de datos.
    Esta API es compartida por Interfaz.html y calculadora.html,
    por lo que 'CUERPO' se sigue consultando para que Interfaz.html funcione.
    calculadora.html simplemente lo ignorará.
    """
    options = {}
    filters = {
        'TIPO': request.args.get('TIPO', '').strip(),
        'FABRICANTE': request.args.get('FABRICANTE', '').strip(),
        'CABEZA': request.args.get('CABEZA', '').strip(),
        'CUERPO': request.args.get('CUERPO', '').strip(), # Se mantiene para Interfaz.html
        'TRAMO': request.args.get('TRAMO', '').strip()
    }

    # 'PARTE_DIVISION' se consulta para que calculadora.html la use
    fields_to_query = ['TIPO', 'FABRICANTE', 'CABEZA', 'CUERPO', 'PARTE_DIVISION', 'TRAMO']
    
    try:
        with engine.connect() as connection:
            
            for field_to_query in fields_to_query:
                query_base = f"SELECT DISTINCT TRIM({field_to_query}) FROM piezas WHERE {field_to_query} IS NOT NULL AND TRIM({field_to_query}) != ''"
                query_params = {}
                
                for filter_field, filter_value in filters.items():
                    if filter_value and filter_field != field_to_query:
                        query_base += f" AND {filter_field} = :{filter_field}"
                        query_params[filter_field] = filter_value
                
                query_base += f" ORDER BY {field_to_query}"
                query = text(query_base)
                result = connection.execute(query, query_params)
                options[field_to_query] = [row[0] for row in result]
                
        return jsonify({
            'success': True,
            'options': options
        })
    except Exception as e:
        print(f"Error al obtener opciones de filtros: {e}")
        return jsonify({
            'success': False,
            'message': 'Error interno del servidor al obtener opciones.'
        }), 500
# -----------------------------------------------------------
# API para Búsqueda (Interfaz.html) - Sin cambios
# -----------------------------------------------------------
@app.route('/api/search', methods=['GET'])
def search_pieces():
    """Ruta API para buscar piezas en la base de datos con búsqueda ESTRICTA."""
    
    tipo = request.args.get('tipo', '').strip()
    fabricante = request.args.get('fabricante', '').strip()
    cabeza = request.args.get('cabeza', '').strip()
    parte = request.args.get('parte', '').strip()
    cuerpo = request.args.get('cuerpo', '').strip()
    tramo = request.args.get('tramo', '').strip()
    
    query_base = "SELECT * FROM piezas WHERE 1=1"
    query_params = {}
    
    if tipo:
        query_base += " AND TIPO = :tipo"
        query_params['tipo'] = tipo
    if fabricante:
        query_base += " AND FABRICANTE = :fabricante"
        query_params['fabricante'] = fabricante
    if cabeza:
        query_base += " AND CABEZA = :cabeza"
        query_params['cabeza'] = cabeza
    if parte:
        query_base += " AND PARTE_DIVISION = :parte"
        query_params['parte'] = parte
    if cuerpo:
        query_base += " AND CUERPO = :cuerpo"
        query_params['cuerpo'] = cuerpo
    if tramo:
        query_base += " AND TRIM(UPPER(TRAMO)) = TRIM(UPPER(:tramo))"
        query_params['tramo'] = tramo
  
    query_base += " LIMIT 500" 
    
    try:
        with engine.connect() as connection:
            result = connection.execute(text(query_base), query_params)
            columns = result.keys()
            pieces = [dict(zip(columns, row)) for row in result]
            
            return jsonify({
                'success': True,
                'count': len(pieces),
                'results': pieces
            })
    except Exception as e:
        print(f"Error en la búsqueda de la base de datos: {e}")
        return jsonify({
            'success': False,
            'message': 'Error interno del servidor al buscar datos.'
        }), 500

# -----------------------------------------------------------
# API para Cálculo (Calculadora.html) - ¡MODIFICADO!
# -----------------------------------------------------------

# --- NUEVO: Definir listas de partes para lógicas especiales ---
# Usamos Sets (conjuntos) para búsquedas más rápidas
PARTS_DIV_2 = {
    'BGDA', 'BSUP', 'BMED', 'BINF', 'BDER', 'BIZQ', 'BSUP/MED'
}
PARTS_DIV_4 = {
    'PATA 0', 'PATA 0.0', 'PATA 1.5', 'PATA 3', 'PATA 3.0', 
    'PATA 4.5', 'PATA 6', 'PATA 6.0', 'PATA 7.5', 'PATA 9', 'PATA 9.0'
}

@app.route('/api/calculate', methods=['POST'])
def calculate_materials():
    """
    Ruta API para calcular materiales con la lógica de división
    solicitada ( /2 y /4 ).
    """
    try:
        data = request.get_json()
        filters = data.get('filters', {})
        parts = data.get('parts', [])
        
        if not parts:
            return jsonify({
                'success': False,
                'message': 'Debe seleccionar al menos una parte'
            }), 400
        
        # 1. Obtener todas las piezas que coincidan con los filtros de torre
        query_base = "SELECT * FROM piezas WHERE 1=1"
        query_params = {}
        
        tipo = filters.get('tipo', '').strip()
        fabricante = filters.get('fabricante', '').strip()
        cabeza = filters.get('cabeza', '').strip()
        # --- ELIMINADO: 'cuerpo' ya no es un filtro aquí ---
        
        if tipo:
            query_base += " AND TIPO = :tipo"
            query_params['tipo'] = tipo
        
        if fabricante:
            query_base += " AND FABRICANTE = :fabricante"
            query_params['fabricante'] = fabricante
        
        if cabeza:
            query_base += " AND CABEZA = :cabeza"
            query_params['cabeza'] = cabeza
        
        with engine.connect() as connection:
            result = connection.execute(text(query_base), query_params)
            columns = result.keys()
            all_pieces = [dict(zip(columns, row)) for row in result]
        
        # 2. Calcular cantidades según partes seleccionadas
        calculated_pieces = []
        
        for piece in all_pieces:
            # Normalizar 'PARTE_DIVISION' de la base de datos
            parte_division_db = str(piece.get('PARTE_DIVISION', '')).strip().upper()

            if not parte_division_db:
                continue # Omitir piezas sin parte_division

            try:
                cantidad_original = float(piece.get('CANTIDAD_X_TORRE', 0))
            except (ValueError, TypeError):
                cantidad_original = 0
                
            cantidad_calculada = 0
            
            # Iterar sobre las partes seleccionadas por el usuario
            for selected_part in parts:
                part_name = str(selected_part.get('part', '')).strip().upper()
                part_qty = selected_part.get('quantity', 0)
                
                # Si la parte de la BD (ej. "A.1") coincide con la parte seleccionada (ej. "A.1")
                if parte_division_db == part_name:
                    
                    # --- NUEVA LÓGICA DE CÁLCULO ---
                    if parte_division_db in PARTS_DIV_2:
                        cantidad_calculada += (cantidad_original * part_qty) / 2
                    elif parte_division_db in PARTS_DIV_4:
                        cantidad_calculada += (cantidad_original * part_qty) / 4
                    else:
                        cantidad_calculada += cantidad_original * part_qty
                    # --- FIN DE NUEVA LÓGICA ---
            
            if cantidad_calculada > 0:
                try:
                    peso_unitario = float(piece.get('PESO_UNITARIO', 0))
                except (ValueError, TypeError):
                    peso_unitario = 0

                peso_total = cantidad_calculada * peso_unitario
                
                calculated_piece = piece.copy()
                calculated_piece['CANTIDAD_ORIGINAL'] = cantidad_original
                calculated_piece['CANTIDAD_CALCULADA'] = cantidad_calculada
                calculated_piece['PESO_TOTAL'] = peso_total
                
                calculated_pieces.append(calculated_piece)
        
        # 3. Calcular totales
        total_piezas = sum(p['CANTIDAD_CALCULADA'] for p in calculated_pieces)
        total_peso = sum(p['PESO_TOTAL'] for p in calculated_pieces)
        
        return jsonify({
            'success': True,
            'count': len(calculated_pieces),
            'results': calculated_pieces,
            'totals': {
                'total_pieces': total_piezas,
                'total_weight': total_peso
            }
        })
        
    except Exception as e:
        print(f"Error en el cálculo de materiales: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': f'Error al calcular materiales: {str(e)}'
        }), 500


if __name__ == '__main__':
    print("\n✨ Servidor Flask iniciando...")
    print("🌐 Buscador: http://127.0.0.1:5000/")
    print("🧮 Calculadora: http://127.0.0.1:5000/calculadora")
    app.run(debug=True)
