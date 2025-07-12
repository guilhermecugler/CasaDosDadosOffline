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