import sqlite3
conn = sqlite3.connect('desglose_torres.db')
cursor = conn.cursor()
cursor.execute("PRAGMA table_info(piezas)")
print(cursor.fetchall())
