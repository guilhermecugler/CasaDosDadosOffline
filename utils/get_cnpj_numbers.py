import sqlite3
import pandas as pd
from threading import Event

def get_cnpj_numbers_sqlite(json_filters, progress_callback, status_callback, cancel_event: Event):
    """
    Busca CNPJs no banco SQLite com base nos filtros fornecidos.
    Respeita o limite máximo de CNPJs definido em json_filters['max_cnpjs'].
    Retorna uma lista de CNPJs que atendem aos critérios.
    """
    db_path = "dados-publicos/cnpj.db"  # Ajuste o caminho conforme necessário
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
    except sqlite3.Error as e:
        status_callback(f"Erro ao conectar ao banco de dados: {e}")
        return []

    query = """
    SELECT DISTINCT e.cnpj
    FROM estabelecimento e
    LEFT JOIN empresas emp ON e.cnpj_basico = emp.cnpj_basico
    LEFT JOIN simples s ON e.cnpj_basico = s.cnpj_basico
    WHERE e.situacao_cadastral = '02'  -- Apenas empresas ativas
    """
    params = []

    query_filters = []

    # Filtro: Termo (razão social ou nome fantasia)
    termo = json_filters['query'].get('termo', [])
    if termo and termo[0]:
        query_filters.append("(emp.razao_social LIKE ? OR e.nome_fantasia LIKE ?)")
        params.extend([f"%{termo[0]}%", f"%{termo[0]}%"])

    # Filtro: Atividade principal (CNAE)
    atividade_principal = json_filters['query'].get('atividade_principal', [])
    if atividade_principal and atividade_principal[0]:
        if json_filters['extras'].get('incluir_atividade_secundaria', False):
            query_filters.append("(e.cnae_fiscal = ? OR e.cnae_fiscal_secundaria LIKE ?)")
            params.extend([atividade_principal[0], f"%{atividade_principal[0]}%"])
        else:
            query_filters.append("e.cnae_fiscal = ?")
            params.append(atividade_principal[0])

    # Filtro: UF
    uf = json_filters['query'].get('uf', [])
    if uf and uf[0]:
        query_filters.append("e.uf = ?")
        params.append(uf[0])

    # Filtro: Município
    municipio = json_filters['query'].get('municipio', [])
    if municipio and municipio[0]:
        cursor.execute("SELECT codigo FROM municipio WHERE descricao = ?", (municipio[0],))
        codigo_municipio = cursor.fetchone()
        if codigo_municipio:
            query_filters.append("e.municipio = ?")
            params.append(codigo_municipio[0])

    # Filtro: CEP
    cep = json_filters['query'].get('cep', [])
    if cep and cep[0]:
        query_filters.append("e.cep = ?")
        params.append(cep[0].replace('-', ''))

    # Filtro: DDD
    ddd = json_filters['query'].get('ddd', [])
    if ddd and ddd[0]:
        query_filters.append("(e.ddd1 = ? OR e.ddd2 = ?)")
        params.extend([ddd[0], ddd[0]])

    # Filtro: Bairro
    bairro = json_filters['query'].get('bairro', [])
    if bairro and bairro[0]:
        query_filters.append("e.bairro LIKE ?")
        params.append(f"%{bairro[0]}%")

    # Filtro: Data de abertura
    data_abertura = json_filters['range_query'].get('data_abertura', {})
    gte = data_abertura.get('gte')
    lte = data_abertura.get('lte')
    if gte:
        query_filters.append("e.data_inicio_atividades >= ?")
        params.append(gte.replace('-', ''))
    if lte:
        query_filters.append("e.data_inicio_atividades <= ?")
        params.append(lte.replace('-', ''))

    # Filtros extras
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

    # Paginação e limite máximo
    page = json_filters.get('page', 1)
    page_size = min(json_filters.get('max_cnpjs', 1000), 1000)  # Máximo de 1000 por página
    max_cnpjs = json_filters.get('max_cnpjs', None)
    if max_cnpjs:
        remaining = max_cnpjs - ((page - 1) * page_size)
        page_size = min(page_size, remaining)
        if page_size <= 0:
            conn.close()
            status_callback(f"Pesquisa finalizada: limite de {max_cnpjs} CNPJs atingido")
            return []
    query += f" LIMIT {page_size} OFFSET {(page - 1) * page_size}"

    try:
        status_callback(f"Consultando página {page}...")
        cursor.execute(query, params)
        cnpjs = [row[0] for row in cursor.fetchall()]
        status_callback(f"Página {page} concluída: {len(cnpjs)} CNPJ(s) encontrados")
    except sqlite3.Error as e:
        status_callback(f"Erro na consulta SQL: {e}")
        cnpjs = []

    conn.close()
    return cnpjs