import sqlite3

def get_cnaes():
    """
    Retorna uma lista de descrições e códigos CNAE do banco SQLite.
    """
    db_path = "dados-publicos/cnpj.db"  # Ajuste o caminho conforme necessário
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT descricao, codigo FROM cnae ORDER BY descricao")
        cnaes = cursor.fetchall()
        conn.close()
        return [['Todas Atividades'] + [c[0] for c in cnaes], [''] + [c[1] for c in cnaes]]
    except sqlite3.Error as e:
        print(f"Erro ao carregar CNAEs: {e}")
        return [['Todas Atividades'], ['']]