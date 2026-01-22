import pandas as pd

def run_etl():
    """
    Implementa el proceso ETL.
    No cambies el nombre de esta función.
    """
    df = pd.read_csv("data/citas_clinica.csv")

    df = df[df['paciente'].notna() & (df['paciente'].str.strip() != '')]
    
    df['fecha_cita'] = pd.to_datetime(df['fecha_cita'], format='%Y-%m-%d', errors='coerce')
    df = df[df['fecha_cita'].notna()]
    df['fecha_cita'] = df['fecha_cita'].dt.strftime('%Y-%m-%d')
    
    df = df[df['costo'] >= 0]
    
    df['paciente'] = df['paciente'].str.title()
    
    df['especialidad'] = df['especialidad'].str.upper()
    
    df['telefono'] = df['telefono'].fillna('NO REGISTRA')
    df.loc[df['telefono'] == '', 'telefono'] = 'NO REGISTRA'
    
    df.to_csv("data/output.csv", index=False)


if __name__ == "__main__":
    run_etl()
