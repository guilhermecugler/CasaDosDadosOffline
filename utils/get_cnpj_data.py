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


def get_all_cnpj_data_sqlite(
    json_filters,
    status_callback,
    fields=None,
    progress_callback=None,
    cancel_event=None,
    limit_hint=None,
):
    """Busca dados completos de CNPJs com filtros avançados."""

    if cancel_event is None:
        cancel_event = Event()

    db_path = "dados-publicos/cnpj.db"
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA temp_store = MEMORY")
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA query_only = 1")
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

    params: list = []
    query_filters: list[str] = []

    termo = json_filters.get('query', {}).get('termo', [])
    if termo and termo[0]:
        query_filters.append("(emp.razao_social LIKE ? OR e.nome_fantasia LIKE ?)")
        params.extend([f"%{termo[0]}%", f"%{termo[0]}%"])

    atividade_principal = json_filters.get('query', {}).get('atividade_principal', [])
    if atividade_principal and atividade_principal[0]:
        if json_filters.get('extras', {}).get('incluir_atividade_secundaria', False):
            query_filters.append("(e.cnae_fiscal = ? OR e.cnae_fiscal_secundaria LIKE ?)")
            params.extend([atividade_principal[0], f"%{atividade_principal[0]}%"])
        else:
            query_filters.append("e.cnae_fiscal = ?")
            params.append(atividade_principal[0])

    uf = json_filters.get('query', {}).get('uf', [])
    if uf and uf[0]:
        query_filters.append("e.uf = ?")
        params.append(uf[0])

    municipio = json_filters.get('query', {}).get('municipio', [])
    if municipio and municipio[0]:
        municipio_value = municipio[0]
        if str(municipio_value).isdigit():
            query_filters.append("e.municipio = ?")
            params.append(int(municipio_value))
        else:
            cursor.execute("SELECT codigo FROM municipio WHERE descricao = ?", (municipio_value.upper(),))
            codigo_municipio = cursor.fetchone()
            if codigo_municipio:
                query_filters.append("e.municipio = ?")
                params.append(codigo_municipio[0])

    cep = json_filters.get('query', {}).get('cep', [])
    if cep and cep[0]:
        query_filters.append("e.cep = ?")
        params.append(cep[0].replace('-', ''))

    ddd = json_filters.get('query', {}).get('ddd', [])
    if ddd and ddd[0]:
        query_filters.append("(e.ddd1 = ? OR e.ddd2 = ?)")
        params.extend([ddd[0], ddd[0]])

    bairro = json_filters.get('query', {}).get('bairro', [])
    if bairro and bairro[0]:
        query_filters.append("e.bairro LIKE ?")
        params.append(f"%{bairro[0]}%")

    data_abertura = json_filters.get('range_query', {}).get('data_abertura', {})
    gte = data_abertura.get('gte')
    lte = data_abertura.get('lte')
    if gte:
        query_filters.append("e.data_inicio_atividades >= ?")
        params.append(gte.replace('-', ''))
    if lte:
        query_filters.append("e.data_inicio_atividades <= ?")
        params.append(lte.replace('-', ''))

    extras = json_filters.get('extras', {})
    telefone_filter = "((e.telefone1 IS NOT NULL AND e.telefone1 != '') OR (e.telefone2 IS NOT NULL AND e.telefone2 != ''))"
    if extras.get('somente_mei'):
        query_filters.append("s.opcao_mei = 'S'")
    if extras.get('excluir_mei'):
        query_filters.append("(s.opcao_mei IS NULL OR s.opcao_mei = 'N')")
    if extras.get('com_email'):
        query_filters.append("e.correio_eletronico IS NOT NULL AND e.correio_eletronico != ''")
    if extras.get('com_contato_telefonico'):
        query_filters.append(telefone_filter)
    if extras.get('somente_fixo'):
        query_filters.append(
            "((e.telefone1 IS NOT NULL AND e.telefone1 != '' AND substr(e.telefone1, 1, 1) IN ('2','3','4','5')) "
            "OR (e.telefone2 IS NOT NULL AND e.telefone2 != '' AND substr(e.telefone2, 1, 1) IN ('2','3','4','5')))"
        )
    if extras.get('somente_celular'):
        query_filters.append(
            "((e.telefone1 IS NOT NULL AND e.telefone1 != '' AND substr(e.telefone1, 1, 1) = '9') "
            "OR (e.telefone2 IS NOT NULL AND e.telefone2 != '' AND substr(e.telefone2, 1, 1) = '9'))"
        )
    if extras.get('somente_matriz'):
        query_filters.append("e.matriz_filial = '1'")
    if extras.get('somente_filial'):
        query_filters.append("e.matriz_filial = '2'")

    if query_filters:
        query += " AND " + " AND ".join(query_filters)

    max_rows = 10000
    if limit_hint:
        try:
            limit_val = int(limit_hint)
            if limit_val > 0:
                max_rows = limit_val
        except (TypeError, ValueError):
            pass
    fetch_limit = max_rows + 1

    query += " ORDER BY e.cnpj LIMIT ?"
    params.append(fetch_limit)

    try:
        cursor.execute(query, params)
    except sqlite3.Error as e:
        status_callback(f"Erro na consulta SQL: {e}")
        conn.close()
        return []

    results = []
    total_fetched = 0
    chunk_size = 1000
    cancelled = False
    if progress_callback:
        progress_callback(0.1)

    while True:
        chunk = cursor.fetchmany(chunk_size)
        if not chunk:
            break
        results.extend(chunk)
        total_fetched += len(chunk)
        if progress_callback and max_rows:
            fetch_ratio = min(total_fetched / max_rows, 1.0)
            progress_callback(0.1 + 0.6 * fetch_ratio)
        if cancel_event.is_set():
            cancelled = True
            break
        if total_fetched >= fetch_limit:
            break

    conn.close()

    if cancelled:
        status_callback("Busca cancelada pelo usuario.")
        return []

    if len(results) >= fetch_limit:
        status_callback(f"Resultado truncado em {max_rows} registros")
        results = results[:max_rows]

    if progress_callback:
        progress_callback(0.75)

    formatted = [dict(zip(selected_fields, row)) for row in results]
    return formatted
