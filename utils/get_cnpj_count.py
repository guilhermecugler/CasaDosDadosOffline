import sqlite3


def get_cnpj_count_sqlite(json_filters, status_callback):
    """Return total distinct CNPJs matching filters in SQLite database."""
    db_path = "dados-publicos/cnpj.db"
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA temp_store = MEMORY")
        conn.execute("PRAGMA synchronous = OFF")
        cursor = conn.cursor()
    except sqlite3.Error as e:
        status_callback(f"Erro ao conectar ao banco de dados: {e}")
        return 0

    query = """
    SELECT COUNT(DISTINCT e.cnpj)
    FROM estabelecimento e
    LEFT JOIN empresas emp ON e.cnpj_basico = emp.cnpj_basico
    LEFT JOIN simples s ON e.cnpj_basico = s.cnpj_basico
    WHERE e.situacao_cadastral = '02'
    """
    params = []
    query_filters = []

    # Termo de busca (razao social ou nome fantasia)
    termo = json_filters['query'].get('termo', [])
    if termo and termo[0]:
        query_filters.append("(emp.razao_social LIKE ? OR e.nome_fantasia LIKE ?)")
        params.extend([f"%{termo[0]}%", f"%{termo[0]}%"])

    # Atividade principal
    atividade_principal = json_filters['query'].get('atividade_principal', [])
    if atividade_principal and atividade_principal[0]:
        if json_filters['extras'].get('incluir_atividade_secundaria', False):
            query_filters.append("(e.cnae_fiscal = ? OR e.cnae_fiscal_secundaria LIKE ?)")
            params.extend([atividade_principal[0], f"%{atividade_principal[0]}%"])
        else:
            query_filters.append("e.cnae_fiscal = ?")
            params.append(atividade_principal[0])

    # UF
    uf = json_filters['query'].get('uf', [])
    if uf and uf[0]:
        query_filters.append("e.uf = ?")
        params.append(uf[0])

    # Municipio
    municipio = json_filters['query'].get('municipio', [])
    if municipio and municipio[0]:
        municipio_value = municipio[0]
        if str(municipio_value).isdigit():
            query_filters.append("e.municipio = ?")
            params.append(int(municipio_value))
        else:
            cursor.execute("SELECT codigo FROM municipio WHERE descricao = ?", (municipio_value,))
            codigo_municipio = cursor.fetchone()
            if codigo_municipio:
                query_filters.append("e.municipio = ?")
                params.append(codigo_municipio[0])

    # CEP
    cep = json_filters['query'].get('cep', [])
    if cep and cep[0]:
        query_filters.append("e.cep = ?")
        params.append(cep[0].replace('-', ''))

    # DDD
    ddd = json_filters['query'].get('ddd', [])
    if ddd and ddd[0]:
        query_filters.append("(e.ddd1 = ? OR e.ddd2 = ?)")
        params.extend([ddd[0], ddd[0]])

    # Bairro
    bairro = json_filters['query'].get('bairro', [])
    if bairro and bairro[0]:
        query_filters.append("e.bairro LIKE ?")
        params.append(f"%{bairro[0]}%")

    # Data de abertura
    data_abertura = json_filters['range_query'].get('data_abertura', {})
    gte = data_abertura.get('gte')
    lte = data_abertura.get('lte')
    if gte:
        query_filters.append("e.data_inicio_atividades >= ?")
        params.append(gte.replace('-', ''))
    if lte:
        query_filters.append("e.data_inicio_atividades <= ?")
        params.append(lte.replace('-', ''))

    # Extras
    if json_filters['extras'].get('somente_mei', False):
        query_filters.append("s.opcao_mei = 'S'")
    if json_filters['extras'].get('excluir_mei', False):
        query_filters.append("(s.opcao_mei IS NULL OR s.opcao_mei = 'N')")
    if json_filters['extras'].get('com_email', False):
        query_filters.append("e.correio_eletronico IS NOT NULL AND e.correio_eletronico != ''")
    if json_filters['extras'].get('com_contato_telefonico', False):
        query_filters.append("(e.telefone1 IS NOT NULL OR e.telefone2 IS NOT NULL)")
    if json_filters['extras'].get('somente_fixo', False):
        query_filters.append("e.ddd1 IN ('11', '12', '13', '14', '15', '16', '17', '18', '19', '21', '22', '24', '27', '28', '31', '32', '33', '34', '35', '37', '38', '41', '42', '43', '44', '45', '46', '47', '48', '49', '51', '53', '54', '55', '61', '62', '63', '64', '65', '66', '67', '68', '69', '71', '73', '74', '75', '77', '79', '81', '82', '83', '84', '85', '86', '87', '88', '89', '91', '92', '93', '94', '95', '96', '97', '98', '99')")
    if json_filters['extras'].get('somente_celular', False):
        query_filters.append("e.ddd1 NOT IN ('11', '12', '13', '14', '15', '16', '17', '18', '19', '21', '22', '24', '27', '28', '31', '32', '33', '34', '35', '37', '38', '41', '42', '43', '44', '45', '46', '47', '48', '49', '51', '53', '54', '55', '61', '62', '63', '64', '65', '66', '67', '68', '69', '71', '73', '74', '75', '77', '79', '81', '82', '83', '84', '85', '86', '87', '88', '89', '91', '92', '93', '94', '95', '96', '97', '98', '99')")
    if json_filters['extras'].get('somente_matriz', False):
        query_filters.append("e.matriz_filial = '1'")
    if json_filters['extras'].get('somente_filial', False):
        query_filters.append("e.matriz_filial = '2'")

    if query_filters:
        query += " AND " + " AND ".join(query_filters)

    try:
        cursor.execute(query, params)
        result = cursor.fetchone()
        total = result[0] if result else 0
        status_callback(f"Total encontrado: {total}")
    except sqlite3.Error as e:
        status_callback(f"Erro na contagem: {e}")
        total = 0
    finally:
        conn.close()

    return total
