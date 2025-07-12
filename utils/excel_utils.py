import pandas as pd

def save_excel(df, file_name):
    """
    Salva o DataFrame em um arquivo Excel.
    """
    try:
        df.to_excel(file_name, index=False, engine='openpyxl')
    except Exception as e:
        raise Exception(f"Erro ao salvar arquivo Excel: {e}")
