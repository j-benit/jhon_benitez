import pandas as pd
import sqlite3
import os

def clean_data():
    # 1. Rutas
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.path.join(base_dir, 'src', 'db', 'ingestion.db')
    clean_xlsx_path = os.path.join(base_dir, 'src', 'xlsx', 'cleaned_data.xlsx')
    report_path = os.path.join(base_dir, 'src', 'static', 'auditoria', 'cleaning_report.txt')

    print("--- Iniciando Preprocesamiento y Limpieza ---")

    # 2. Carga de datos (Simulación de Cloud Ingest)
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM posts", conn)
    conn.close()

    registros_iniciales = len(df)

    # 3. Limpieza y Transformación
    # a. Eliminación de duplicados
    df = df.drop_duplicates()
    
    # b. Manejo de valores nulos (Imputación o eliminación)
    # Llenamos nulos en 'title' con "Sin título" si existieran
    df['title'] = df['title'].fillna("Sin título")
    df['body'] = df['body'].fillna("Sin contenido")
    
    # c. Corrección de tipos de datos
    df['userId'] = df['userId'].astype(int)
    df['id'] = df['id'].astype(int)

    # d. Transformación adicional: Normalización (Pasar títulos a mayúsculas)
    df['title'] = df['title'].str.upper()

    registros_finales = len(df)

    # 4. Exportar Datos Limpios
    df.head(20).to_excel(clean_xlsx_path, index=False)
    print(f"Muestra limpia guardada en: {clean_xlsx_path}")

    # 5. Generar Reporte de Auditoría (.txt)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("REPORTE DE LIMPIEZA Y PREPROCESAMIENTO\n")
        f.write("="*40 + "\n")
        f.write(f"Registros iniciales: {registros_iniciales}\n")
        f.write(f"Registros después de limpieza: {registros_finales}\n")
        f.write(f"Duplicados eliminados: {registros_iniciales - registros_finales}\n")
        f.write("\nOPERACIONES REALIZADAS:\n")
        f.write("- Eliminación de duplicados (drop_duplicates)\n")
        f.write("- Imputación de valores nulos en 'title' y 'body'\n")
        f.write("- Conversión de tipos de datos a INT para IDs\n")
        f.write("- Normalización: Títulos convertidos a MAYÚSCULAS\n")
        f.write("-" * 40 + "\n")
        f.write("Estado: DATOS CONSISTENTES PARA MODELADO\n")

    print(f"Reporte de limpieza generado en: {report_path}")

if __name__ == "__main__":
    clean_data()