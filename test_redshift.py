import psycopg2

# Parámetros de conexión (modifica con los tuyos)
DB_NAME = "transcripted"
USER = "dlp_admin"
PASSWORD = "axUMOgZgOfOsY7J4yHfO"
HOST = "redshift-datalakep-cluster.chqburshgayp.us-west-2.redshift.amazonaws.com"
PORT = "5439"

# Establecer conexión
try:
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT
    )
    print("Conexión exitosa a Redshift")

    # Crear un cursor
    cur = conn.cursor()

    # Crear una tabla de prueba
    cur.execute("""
        CREATE TABLE IF NOT EXISTS test_table (
            id INT PRIMARY KEY,
            name VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("Tabla creada exitosamente")

    # Insertar un dato de prueba
    cur.execute("INSERT INTO test_table (id, name) VALUES (%s, %s)", (1, 'Ejemplo'))
    print("Dato insertado correctamente")

    # Confirmar los cambios
    conn.commit()

    # Consultar los datos
    cur.execute("SELECT * FROM test_table")
    rows = cur.fetchall()
    for row in rows:
        print(row)

    # Cerrar la conexión
    cur.close()
    conn.close()
    print("Conexión cerrada")

except Exception as e:
    print("Error al conectar a Redshift:", e)
