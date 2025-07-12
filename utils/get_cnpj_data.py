import sqlite3
import pandas as pd
from threading import Event
from .excel_utils import save_excel

def get_cnpj_data_sqlite(cnpjs, file_name, status_callback, cancel_event: Event):
    """
    Busca detalhes dos CNPJs no banco SQLite e salva em um arquivo.
    Retorna a quantidade de CNPJs processados.
    """
    if not cnpjs:
        status_callback("Nenhum CNPJ para processar")
        return 0

    db_path = "dados-publicos/cnpj.db"  # Ajuste o caminho conforme necessário
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
    except sqlite3.Error as e:
        status_callback(f"Erro ao conectar ao banco de dados: {e}")
        return 0

    query = """
    SELECT 
        e.cnpj,
        emp.razao_social,
        e.nome_fantasia,
        e.cnae_fiscal,
        cnae.descricao AS cnae_descricao,
        e.cnae_fiscal_secundaria,
        e.tipo_logradouro || ' ' || e.logradouro || ', ' || e.numero || ' ' || e.complemento AS endereco,
        e.bairro,
        e.cep,
        e.uf,
        m.descricao AS municipio,
        e.ddd1 || ' ' || e.telefone1 AS telefone1,
        e.ddd2 || ' ' || e.telefone2 AS telefone2,
        e.correio_eletronico,
        e.data_inicio_atividades,
        emp.capital_social,
        s.opcao_mei
    FROM estabelecimento e
    LEFT JOIN empresas emp ON e.cnpj_basico = emp.cnpj_basico
    LEFT JOIN simples s ON e.cnpj_basico = s.cnpj_basico
    LEFT JOIN cnae ON e.cnae_fiscal = cnae.codigo
    LEFT JOIN municipio m ON e.municipio = m.codigo
    WHERE e.cnpj IN ({})
    """
    placeholders = ','.join(['?' for _ in cnpjs])
    query = query.format(placeholders)

    try:
        cursor.execute(query, cnpjs)
        rows = cursor.fetchall()
        columns = [
            'CNPJ', 'Razão Social', 'Nome Fantasia', 'CNAE Fiscal', 'Descrição CNAE',
            'CNAE Secundária', 'Endereço', 'Bairro', 'CEP', 'UF', 'Município',
            'Telefone 1', 'Telefone 2', 'E-mail', 'Data Abertura', 'Capital Social', 'MEI'
        ]
        df = pd.DataFrame(rows, columns=columns)
    except sqlite3.Error as e:
        status_callback(f"Erro ao buscar detalhes: {e}")
        df = pd.DataFrame()
    finally:
        conn.close()

    if not df.empty and not cancel_event.is_set():
        try:
            if file_name.endswith('.xlsx'):
                save_excel(df, file_name)
            else:
                df.to_csv(file_name, index=False, encoding='utf-8')
            status_callback(f"Salvo {len(df)} CNPJ(s) em {file_name}")
        except Exception as e:
            status_callback(f"Erro ao salvar arquivo: {e}")
    else:
        status_callback("Nenhum dado para salvar")

    return len(df)


def get_all_cnpj_data_sqlite(json_filters, status_callback, fields=None):
    """Busca dados completos de CNPJs com filtros avançados.

    Parameters
    ----------
    json_filters : dict
        Dicionário de filtros conforme o formato utilizado no projeto.
    status_callback : Callable[[str], None]
        Função chamada para relatar o status da consulta.
    fields : list[str], optional
        Lista opcional de campos a serem retornados. Quando ``None`` todos os
        campos padrão são utilizados.

    Returns
    -------
    list[dict]
        Lista de dicionários com os dados encontrados (no máximo 10.000
        registros). Caso existam mais resultados, é enviado um aviso via
        ``status_callback``.

    Example
    -------
    >>> data = get_all_cnpj_data_sqlite(json_filters, print)
    >>> print(len(data))
    """

    db_path = "dados-publicos/cnpj.db"
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA temp_store = MEMORY")
        conn.execute("PRAGMA synchronous = OFF")
        cursor = conn.cursor()
    except sqlite3.Error as e:
        status_callback(f"Erro ao conectar ao banco de dados: {e}")
        return []

    column_map = {
        "cnpj": "e.cnpj AS cnpj",
        "razao_social": "emp.razao_social AS razao_social",
        "nome_fantasia": "e.nome_fantasia AS nome_fantasia",
        "cnae_fiscal": "e.cnae_fiscal AS cnae_fiscal",
        "cnae_descricao": "cnae.descricao AS cnae_descricao",
        "cnae_secundaria": "e.cnae_fiscal_secundaria AS cnae_secundaria",
        "endereco": "e.tipo_logradouro || ' ' || e.logradouro || ', ' || e.numero || ' ' || e.complemento AS endereco",
        "bairro": "e.bairro AS bairro",
        "cep": "e.cep AS cep",
        "uf": "e.uf AS uf",
        "municipio": "m.descricao AS municipio",
        "telefone1": "e.ddd1 || ' ' || e.telefone1 AS telefone1",
        "telefone2": "e.ddd2 || ' ' || e.telefone2 AS telefone2",
        "email": "e.correio_eletronico AS email",
        "data_abertura": "e.data_inicio_atividades AS data_abertura",
        "capital_social": "emp.capital_social AS capital_social",
        "mei": "s.opcao_mei AS mei",
    }

    selected_fields = fields or list(column_map.keys())
    select_clause = ", ".join(column_map[f] for f in selected_fields if f in column_map)

    query = f"""
    SELECT {select_clause}
    FROM estabelecimento e
    LEFT JOIN empresas emp ON e.cnpj_basico = emp.cnpj_basico
    LEFT JOIN simples s ON e.cnpj_basico = s.cnpj_basico
    LEFT JOIN cnae ON e.cnae_fiscal = cnae.codigo
    LEFT JOIN municipio m ON e.municipio = m.codigo
    WHERE e.situacao_cadastral = '02'
    """

    params = []
    query_filters = []

    # Termo de busca
    termo = json_filters.get('query', {}).get('termo', [])
    if termo and termo[0]:
        query_filters.append("(emp.razao_social LIKE ? OR e.nome_fantasia LIKE ?)")
        params.extend([f"%{termo[0]}%", f"%{termo[0]}%"])

    # Atividade principal (CNAE)
    atividade_principal = json_filters.get('query', {}).get('atividade_principal', [])
    if atividade_principal and atividade_principal[0]:
        if json_filters.get('extras', {}).get('incluir_atividade_secundaria', False):
            query_filters.append("(e.cnae_fiscal = ? OR e.cnae_fiscal_secundaria LIKE ?)")
            params.extend([atividade_principal[0], f"%{atividade_principal[0]}%"])
        else:
            query_filters.append("e.cnae_fiscal = ?")
            params.append(atividade_principal[0])

    # UF
    uf = json_filters.get('query', {}).get('uf', [])
    if uf and uf[0]:
        query_filters.append("e.uf = ?")
        params.append(uf[0])

    # Municipio
    municipio = json_filters.get('query', {}).get('municipio', [])
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
    cep = json_filters.get('query', {}).get('cep', [])
    if cep and cep[0]:
        query_filters.append("e.cep = ?")
        params.append(cep[0].replace('-', ''))

    # DDD
    ddd = json_filters.get('query', {}).get('ddd', [])
    if ddd and ddd[0]:
        query_filters.append("(e.ddd1 = ? OR e.ddd2 = ?)")
        params.extend([ddd[0], ddd[0]])

    # Bairro
    bairro = json_filters.get('query', {}).get('bairro', [])
    if bairro and bairro[0]:
        query_filters.append("e.bairro LIKE ?")
        params.append(f"%{bairro[0]}%")

    # Data de abertura
    data_abertura = json_filters.get('range_query', {}).get('data_abertura', {})
    gte = data_abertura.get('gte')
    lte = data_abertura.get('lte')
    if gte:
        query_filters.append("e.data_inicio_atividades >= ?")
        params.append(gte.replace('-', ''))
    if lte:
        query_filters.append("e.data_inicio_atividades <= ?")
        params.append(lte.replace('-', ''))

    # Extras
    extras = json_filters.get('extras', {})
    if extras.get('somente_mei'):
        query_filters.append("s.opcao_mei = 'S'")
    if extras.get('excluir_mei'):
        query_filters.append("(s.opcao_mei IS NULL OR s.opcao_mei = 'N')")
    if extras.get('com_email'):
        query_filters.append("e.correio_eletronico IS NOT NULL AND e.correio_eletronico != ''")
    if extras.get('com_contato_telefonico'):
        query_filters.append("(e.telefone1 IS NOT NULL OR e.telefone2 IS NOT NULL)")
    if extras.get('somente_fixo'):
        query_filters.append("e.ddd1 IN ('11', '12', '13', '14', '15', '16', '17', '18', '19', '21', '22', '24', '27', '28', '31', '32', '33', '34', '35', '37', '38', '41', '42', '43', '44', '45', '46', '47', '48', '49', '51', '53', '54', '55', '61', '62', '63', '64', '65', '66', '67', '68', '69', '71', '73', '74', '75', '77', '79', '81', '82', '83', '84', '85', '86', '87', '88', '89', '91', '92', '93', '94', '95', '96', '97', '98', '99')")
    if extras.get('somente_celular'):
        query_filters.append("e.ddd1 NOT IN ('11', '12', '13', '14', '15', '16', '17', '18', '19', '21', '22', '24', '27', '28', '31', '32', '33', '34', '35', '37', '38', '41', '42', '43', '44', '45', '46', '47', '48', '49', '51', '53', '54', '55', '61', '62', '63', '64', '65', '66', '67', '68', '69', '71', '73', '74', '75', '77', '79', '81', '82', '83', '84', '85', '86', '87', '88', '89', '91', '92', '93', '94', '95', '96', '97', '98', '99')")
    if extras.get('somente_matriz'):
        query_filters.append("e.matriz_filial = '1'")
    if extras.get('somente_filial'):
        query_filters.append("e.matriz_filial = '2'")

    if query_filters:
        query += " AND " + " AND ".join(query_filters)

    # Busca no banco (limite de 10.000 registros)
    limit = 10001
    query += f" LIMIT {limit}"

    try:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    except sqlite3.Error as e:
        status_callback(f"Erro na consulta SQL: {e}")
        conn.close()
        return []
    finally:
        conn.close()

    if len(rows) == limit:
        status_callback("Resultado truncado em 10.000 registros")
        rows = rows[:10000]

    results = [dict(zip(selected_fields, row)) for row in rows]
    return results