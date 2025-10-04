from __future__ import annotations

import io
import os
import re
import shutil
import sqlite3
import zipfile
from datetime import datetime
from pathlib import Path
from threading import Event
from typing import Callable, Dict, Iterator, List, Optional, Sequence, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) CasaDosDadosOffline/1.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

CONNECT_TIMEOUT = 20
READ_TIMEOUT = 120
STREAM_CHUNK = 4 * 1024 * 1024
CSV_CHUNK_ROWS = 100000

StatusCallback = Callable[[str], None]
ProgressCallback = Optional[Callable[[float], None]]


class UpdateCancelled(Exception):
    """Raised when the user cancels the database update."""


class DatabaseUpdateError(Exception):
    """Raised when the database update fails."""


class ProgressReporter:
    def __init__(self, callback: ProgressCallback):
        self._callback = callback
        self._total = 1
        self._completed = 0

    def set_total(self, total: int) -> None:
        self._total = max(total, 1)
        self._completed = 0
        self._report()

    def increment(self, step: int = 1) -> None:
        self._completed += step
        self._report()

    def complete(self) -> None:
        self._completed = self._total
        self._report()

    def _report(self) -> None:
        if not self._callback:
            return
        value = 0.0 if self._total == 0 else min(max(self._completed / self._total, 0.0), 1.0)
        self._callback(value)


CODE_TABLES = [
    {"table": "cnae", "zip_name": "cnaes.zip", "suffixes": (".cnaecsv",)},
    {"table": "motivo", "zip_name": "motivos.zip", "suffixes": (".moticsv",)},
    {"table": "municipio", "zip_name": "municipios.zip", "suffixes": (".municcsv",)},
    {"table": "natureza_juridica", "zip_name": "naturezas.zip", "suffixes": (".natjucsv",)},
    {"table": "pais", "zip_name": "paises.zip", "suffixes": (".paiscsv",)},
    {"table": "qualificacao_socio", "zip_name": "qualificacoes.zip", "suffixes": (".qualscsv",)},
]

EMPRESAS_COLUMNS = [
    "cnpj_basico",
    "razao_social",
    "natureza_juridica",
    "qualificacao_responsavel",
    "capital_social_str",
    "porte_empresa",
    "ente_federativo_responsavel",
]

ESTABELECIMENTO_COLUMNS = [
    "cnpj_basico",
    "cnpj_ordem",
    "cnpj_dv",
    "matriz_filial",
    "nome_fantasia",
    "situacao_cadastral",
    "data_situacao_cadastral",
    "motivo_situacao_cadastral",
    "nome_cidade_exterior",
    "pais",
    "data_inicio_atividades",
    "cnae_fiscal",
    "cnae_fiscal_secundaria",
    "tipo_logradouro",
    "logradouro",
    "numero",
    "complemento",
    "bairro",
    "cep",
    "uf",
    "municipio",
    "ddd1",
    "telefone1",
    "ddd2",
    "telefone2",
    "ddd_fax",
    "fax",
    "correio_eletronico",
    "situacao_especial",
    "data_situacao_especial",
]

SOCIOS_COLUMNS = [
    "cnpj_basico",
    "identificador_de_socio",
    "nome_socio",
    "cnpj_cpf_socio",
    "qualificacao_socio",
    "data_entrada_sociedade",
    "pais",
    "representante_legal",
    "nome_representante",
    "qualificacao_representante_legal",
    "faixa_etaria",
]

SIMPLES_COLUMNS = [
    "cnpj_basico",
    "opcao_simples",
    "data_opcao_simples",
    "data_exclusao_simples",
    "opcao_mei",
    "data_opcao_mei",
    "data_exclusao_mei",
]

EMPRESAS_SUFFIXES = (".emprecsv",)
ESTABELECIMENTO_SUFFIXES = (".estabele",)
SOCIOS_SUFFIXES = (".sociocsv",)
SIMPLES_SUFFIXES = (".simples.csv", ".simples")

__all__ = ["update_cnpj_database", "DatabaseUpdateError", "UpdateCancelled"]


def update_cnpj_database(
    status_callback: StatusCallback,
    progress_callback: ProgressCallback = None,
    cancel_event: Optional[Event] = None,
    cleanup: bool = True,
) -> Path:
    """Download Receita Federal data and rebuild the sqlite database."""

    cancel_event = cancel_event or Event()
    project_root = Path(__file__).resolve().parent.parent
    zip_dir = project_root / "dados-publicos-zip"
    data_dir = project_root / "dados-publicos"
    zip_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "cnpj.db"

    progress = ProgressReporter(progress_callback)

    try:
        reference, remote_modified, remote_files = _fetch_remote_dataset_info()
        local_reference = _get_local_reference_date(db_path)

        if local_reference is not None:
            local_token = f"{local_reference.year:04d}-{local_reference.month:02d}"
            if reference == local_token or (
                remote_modified and local_reference.date() >= remote_modified.date()
            ):
                progress.set_total(1)
                progress.complete()
                status_callback(
                    f"Base offline ja esta atualizada (dados de {local_reference.strftime('%d/%m/%Y')})."
                )
                return db_path

        categories = _categorize_remote_files(remote_files)
        include_socios = bool(categories["socios"])
        conversion_steps = _estimate_conversion_steps(categories, include_socios)
        progress.set_total(len(remote_files) + conversion_steps)

        status_callback(f"Atualizando base de dados ({reference})...")
        _prepare_directory(zip_dir)

        downloaded = _download_remote_files(
            remote_files, zip_dir, status_callback, progress, cancel_event
        )
        db_path = _build_sqlite_database(
            downloaded,
            categories,
            data_dir,
            reference,
            status_callback,
            progress,
            cancel_event,
        )

        if cleanup:
            _prepare_directory(zip_dir)

        progress.complete()
        status_callback("Banco atualizado com sucesso!")
        return db_path
    except UpdateCancelled:
        status_callback("Atualizacao cancelada.")
        raise
    except requests.RequestException as exc:
        raise DatabaseUpdateError(f"Erro de rede ao acessar os dados da Receita Federal: {exc}") from exc
    except (sqlite3.Error, zipfile.BadZipFile, OSError, pd.errors.ParserError) as exc:
        raise DatabaseUpdateError(f"Falha ao construir o banco de dados: {exc}") from exc



def _prepare_directory(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for entry in directory.iterdir():
        if entry.is_file() or entry.is_symlink():
            entry.unlink()
        else:
            shutil.rmtree(entry)


def _fetch_remote_dataset_info() -> Tuple[str, Optional[datetime], List[Tuple[str, str]]]:
    reference, modified = _fetch_latest_remote_reference()
    files = _fetch_dataset_files(reference)
    return reference, modified, files


def _fetch_latest_remote_reference() -> Tuple[str, Optional[datetime]]:
    listing = requests.get(
        f"{BASE_URL}?C=M;O=D", headers=HTTP_HEADERS, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
    )
    listing.raise_for_status()
    soup = BeautifulSoup(listing.text, "html.parser")
    pattern = re.compile(r"^\d{4}-\d{2}/$")
    for row in soup.find_all("tr"):
        link = row.find("a")
        if not link:
            continue
        href = link.get("href", "")
        if not pattern.match(href):
            continue
        reference = href.strip("/")
        last_modified = None
        cells = row.find_all("td")
        if len(cells) >= 3:
            raw = cells[2].get_text(strip=True)
            if raw:
                try:
                    last_modified = datetime.strptime(raw, "%Y-%m-%d %H:%M")
                except ValueError:
                    last_modified = None
        return reference, last_modified
    raise DatabaseUpdateError(
        "Nao foi possivel identificar a ultima versao da base na Receita Federal."
    )


def _fetch_dataset_files(reference: str) -> List[Tuple[str, str]]:
    normalized = reference.strip("/")
    dataset_url = f"{BASE_URL}{normalized}/"
    dataset_listing = requests.get(
        dataset_url, headers=HTTP_HEADERS, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
    )
    dataset_listing.raise_for_status()
    soup = BeautifulSoup(dataset_listing.text, "html.parser")

    files: List[Tuple[str, str]] = []
    for link in soup.find_all("a"):
        href = link.get("href")
        if not href or not href.lower().endswith(".zip"):
            continue
        url = href if href.startswith("http") else dataset_url + href
        files.append((href, url))

    if not files:
        raise DatabaseUpdateError(f"Nenhum arquivo zip encontrado em {dataset_url}.")

    files.sort(key=lambda item: item[0].lower())
    return files


def _get_local_reference_date(db_path: Path) -> Optional[datetime]:
    if not db_path.exists():
        return None
    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                "SELECT valor FROM _referencia WHERE referencia = 'CNPJ'"
            ).fetchone()
    except sqlite3.Error:
        return None
    if not row:
        return None
    raw_value = str(row[0] or '').strip()
    if not raw_value:
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw_value, fmt)
        except ValueError:
            continue
    return None


def _categorize_remote_files(remote_files: Sequence[Tuple[str, str]]) -> Dict[str, object]:
    names = [name for name, _ in remote_files]
    categories: Dict[str, object] = {
        "empresas": sorted([name for name in names if name.lower().startswith("empresas")]),
        "estabelecimentos": sorted([name for name in names if name.lower().startswith("estabelecimentos")]),
        "socios": sorted([name for name in names if name.lower().startswith("socios")]),
        "simples": sorted([name for name in names if name.lower().startswith("simples")]),
        "code_tables": {},
    }

    lookup = {name.lower(): name for name in names}
    code_tables: Dict[str, Optional[str]] = {}
    for config in CODE_TABLES:
        code_tables[config["table"]] = lookup.get(config["zip_name"], None)
    categories["code_tables"] = code_tables
    return categories


def _estimate_conversion_steps(categories: Dict[str, object], include_socios: bool) -> int:
    steps = len(categories["empresas"]) + len(categories["estabelecimentos"]) + len(categories["simples"])
    if include_socios:
        steps += len(categories["socios"])
    code_tables: Dict[str, Optional[str]] = categories["code_tables"]  # type: ignore[assignment]
    steps += sum(1 for value in code_tables.values() if value)
    tasks = _build_post_sql_tasks(include_socios)
    steps += len(tasks)
    steps += 1  # referencia
    steps += 1  # replace final
    return steps


def _download_remote_files(
    remote_files: Sequence[Tuple[str, str]],
    destination: Path,
    status_callback: StatusCallback,
    progress: ProgressReporter,
    cancel_event: Optional[Event],
) -> List[Tuple[str, Path]]:
    downloaded: List[Tuple[str, Path]] = []
    total = len(remote_files)
    for index, (name, url) in enumerate(remote_files, start=1):
        _check_cancel(cancel_event)
        status_callback(f"Baixando {name} ({index}/{total})...")
        local_path = destination / name
        _stream_download(url, local_path, cancel_event)
        downloaded.append((name, local_path))
        progress.increment()
    return downloaded


def _stream_download(url: str, destination: Path, cancel_event: Optional[Event]) -> None:
    temporary = destination.with_suffix(destination.suffix + ".part")
    if temporary.exists():
        temporary.unlink()

    try:
        with requests.get(url, headers=HTTP_HEADERS, stream=True, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)) as response:
            response.raise_for_status()
            with open(temporary, "wb") as handler:
                for chunk in response.iter_content(STREAM_CHUNK):
                    _check_cancel(cancel_event)
                    if chunk:
                        handler.write(chunk)
        os.replace(temporary, destination)
    except Exception:
        if temporary.exists():
            temporary.unlink()
        raise


def _build_sqlite_database(
    downloaded_files: Sequence[Tuple[str, Path]],
    categories: Dict[str, object],
    data_dir: Path,
    reference: str,
    status_callback: StatusCallback,
    progress: ProgressReporter,
    cancel_event: Optional[Event],
) -> Path:
    entry_map = {name.lower(): path for name, path in downloaded_files}

    empresas_paths = [_require_entry_path(entry_map, name) for name in categories["empresas"]]
    estab_paths = [_require_entry_path(entry_map, name) for name in categories["estabelecimentos"]]
    socios_paths = [_require_entry_path(entry_map, name) for name in categories["socios"]]
    simples_paths = [_require_entry_path(entry_map, name) for name in categories["simples"]]

    code_table_configs: List[Tuple[str, Path, Tuple[str, ...]]] = []
    code_tables: Dict[str, Optional[str]] = categories["code_tables"]  # type: ignore[assignment]
    for config in CODE_TABLES:
        zip_name = code_tables.get(config["table"])
        if zip_name:
            code_table_configs.append((config["table"], _require_entry_path(entry_map, zip_name), config["suffixes"]))

    tmp_path = data_dir / "cnpj.db.tmp"
    if tmp_path.exists():
        tmp_path.unlink()

    conn = sqlite3.connect(tmp_path)
    try:
        _configure_database(conn)

        for table_name, zip_path, suffixes in code_table_configs:
            _check_cancel(cancel_event)
            status_callback(f"Convertendo {zip_path.name}...")
            _load_code_table(conn, table_name, zip_path, suffixes, status_callback, progress, cancel_event)

        if empresas_paths:
            _load_large_table(
                conn,
                "empresas",
                empresas_paths,
                EMPRESAS_COLUMNS,
                EMPRESAS_SUFFIXES,
                status_callback,
                progress,
                cancel_event,
            )

        if estab_paths:
            _load_large_table(
                conn,
                "estabelecimento",
                estab_paths,
                ESTABELECIMENTO_COLUMNS,
                ESTABELECIMENTO_SUFFIXES,
                status_callback,
                progress,
                cancel_event,
            )

        if simples_paths:
            _load_large_table(
                conn,
                "simples",
                simples_paths,
                SIMPLES_COLUMNS,
                SIMPLES_SUFFIXES,
                status_callback,
                progress,
                cancel_event,
            )

        include_socios = bool(socios_paths)
        if include_socios:
            _load_large_table(
                conn,
                "socios_original",
                socios_paths,
                SOCIOS_COLUMNS,
                SOCIOS_SUFFIXES,
                status_callback,
                progress,
                cancel_event,
            )

        tasks = _build_post_sql_tasks(include_socios)
        for message, statement in tasks:
            _check_cancel(cancel_event)
            status_callback(f"{message}...")
            conn.execute(statement)
            conn.commit()
            progress.increment()

        reference_date = _extract_reference_date(empresas_paths) if empresas_paths else datetime.now().strftime("%d/%m/%Y")
        total_estabelecimentos = conn.execute("SELECT COUNT(*) FROM estabelecimento").fetchone()[0]
        conn.execute("DELETE FROM _referencia")
        conn.execute("INSERT INTO _referencia (referencia, valor) VALUES (?, ?)", ("CNPJ", reference_date))
        conn.execute(
            "INSERT INTO _referencia (referencia, valor) VALUES (?, ?)",
            ("cnpj_qtde", str(total_estabelecimentos)),
        )
        conn.commit()
        progress.increment()
    except Exception:
        conn.close()
        if tmp_path.exists():
            tmp_path.unlink()
        raise

    conn.close()

    final_path = data_dir / "cnpj.db"
    if final_path.exists():
        final_path.unlink()
    os.replace(tmp_path, final_path)
    progress.increment()
    status_callback(f"Banco atualizado: {final_path}")
    return final_path


def _configure_database(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode = OFF")
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA cache_size = -64000")


def _load_code_table(
    conn: sqlite3.Connection,
    table_name: str,
    zip_path: Path,
    suffixes: Tuple[str, ...],
    status_callback: StatusCallback,
    progress: ProgressReporter,
    cancel_event: Optional[Event],
) -> None:
    _check_cancel(cancel_event)
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute(f"CREATE TABLE {table_name} (codigo TEXT, descricao TEXT)")

    with zipfile.ZipFile(zip_path) as archive:
        member = _resolve_csv_member(archive, suffixes)
        with archive.open(member) as raw:
            wrapper = io.TextIOWrapper(raw, encoding="latin1", newline="")
            data = pd.read_csv(
                wrapper,
                sep=";",
                header=None,
                names=["codigo", "descricao"],
                dtype=str,
                na_filter=False,
                engine="python",
            )
    conn.executemany(
        f"INSERT INTO {table_name} (codigo, descricao) VALUES (?, ?)",
        data.itertuples(index=False, name=None),
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name} ON {table_name}(codigo)")
    conn.commit()
    progress.increment()


def _load_large_table(
    conn: sqlite3.Connection,
    table_name: str,
    zip_paths: Sequence[Path],
    columns: Sequence[str],
    suffixes: Tuple[str, ...],
    status_callback: StatusCallback,
    progress: ProgressReporter,
    cancel_event: Optional[Event],
) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    columns_sql = ", ".join(f"{column} TEXT" for column in columns)
    conn.execute(f"CREATE TABLE {table_name} ({columns_sql})")
    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})"
    cursor = conn.cursor()

    for zip_path in zip_paths:
        _check_cancel(cancel_event)
        status_callback(f"Processando {zip_path.name} em {table_name}...")
        conn.execute("BEGIN")
        try:
            for chunk in _iter_csv_chunks(zip_path, suffixes, columns):
                _check_cancel(cancel_event)
                cursor.executemany(insert_sql, chunk.itertuples(index=False, name=None))
        except Exception:
            conn.rollback()
            raise
        else:
            conn.commit()
        progress.increment()

    cursor.close()


def _iter_csv_chunks(
    zip_path: Path,
    suffixes: Tuple[str, ...],
    columns: Sequence[str],
) -> Iterator[pd.DataFrame]:
    with zipfile.ZipFile(zip_path) as archive:
        member = _resolve_csv_member(archive, suffixes)
        with archive.open(member) as raw:
            wrapper = io.TextIOWrapper(raw, encoding="latin1", newline="")
            reader = pd.read_csv(
                wrapper,
                sep=";",
                header=None,
                names=columns,
                dtype=str,
                na_filter=False,
                engine="python",
                chunksize=CSV_CHUNK_ROWS,
            )
            for chunk in reader:
                yield chunk


def _resolve_csv_member(archive: zipfile.ZipFile, suffixes: Tuple[str, ...]) -> str:
    suffixes_lower = tuple(suffix.lower() for suffix in suffixes)
    for member in archive.namelist():
        if member.lower().endswith(suffixes_lower):
            return member
    raise DatabaseUpdateError("Arquivo CSV esperado nao encontrado no pacote da Receita Federal.")


def _extract_reference_date(empresas_paths: Sequence[Path]) -> str:
    for path in empresas_paths:
        try:
            with zipfile.ZipFile(path) as archive:
                for member in archive.namelist():
                    parts = member.split(".")
                    if len(parts) >= 3 and parts[2].startswith("D") and len(parts[2]) >= 6:
                        token = parts[2]
                        day = token[4:6]
                        month = token[2:4]
                        year = f"202{token[1]}"
                        return f"{day}/{month}/{year}"
        except zipfile.BadZipFile:
            continue
    return datetime.now().strftime("%d/%m/%Y")


def _check_cancel(cancel_event: Optional[Event]) -> None:
    if cancel_event and cancel_event.is_set():
        raise UpdateCancelled()


def _require_entry_path(entry_map: Dict[str, Path], name: str) -> Path:
    key = name.lower()
    if key not in entry_map:
        raise DatabaseUpdateError(f"Arquivo {name} nao foi baixado corretamente.")
    return entry_map[key]


def _build_post_sql_tasks(include_socios: bool) -> List[Tuple[str, str]]:
    tasks: List[Tuple[str, str]] = [
        ("Criando coluna capital_social", "ALTER TABLE empresas ADD COLUMN capital_social REAL"),
        (
            "Atualizando capital_social",
            "UPDATE empresas SET capital_social = CAST(REPLACE(capital_social_str, ',', '.') AS REAL)",
        ),
        ("Removendo capital_social_str", "ALTER TABLE empresas DROP COLUMN capital_social_str"),
        ("Criando coluna cnpj em estabelecimento", "ALTER TABLE estabelecimento ADD COLUMN cnpj TEXT"),
        (
            "Preenchendo coluna cnpj em estabelecimento",
            "UPDATE estabelecimento SET cnpj = cnpj_basico || cnpj_ordem || cnpj_dv",
        ),
        (
            "Indexando empresas",
            "CREATE INDEX IF NOT EXISTS idx_empresas_cnpj_basico ON empresas (cnpj_basico)",
        ),
        (
            "Indexando empresas por razao social",
            "CREATE INDEX IF NOT EXISTS idx_empresas_razao_social ON empresas (razao_social)",
        ),
        (
            "Indexando estabelecimento por cnpj_basico",
            "CREATE INDEX IF NOT EXISTS idx_estabelecimento_cnpj_basico ON estabelecimento (cnpj_basico)",
        ),
        (
            "Indexando estabelecimento por cnpj",
            "CREATE INDEX IF NOT EXISTS idx_estabelecimento_cnpj ON estabelecimento (cnpj)",
        ),
        (
            "Indexando estabelecimento por nome fantasia",
            "CREATE INDEX IF NOT EXISTS idx_estabelecimento_nomefantasia ON estabelecimento (nome_fantasia)",
        ),
    ]

    if include_socios:
        tasks.extend(
            [
                (
                    "Indexando socios_original",
                    "CREATE INDEX IF NOT EXISTS idx_socios_original_cnpj_basico ON socios_original(cnpj_basico)",
                ),
                (
                    "Gerando tabela de socios",
                    "CREATE TABLE IF NOT EXISTS socios AS SELECT te.cnpj AS cnpj, ts.* FROM socios_original ts "
                    "LEFT JOIN estabelecimento te ON te.cnpj_basico = ts.cnpj_basico WHERE te.matriz_filial = '1'",
                ),
                ("Removendo socios_original", "DROP TABLE IF EXISTS socios_original"),
                ("Indexando socios por cnpj", "CREATE INDEX IF NOT EXISTS idx_socios_cnpj ON socios(cnpj)"),
                (
                    "Indexando socios por documento",
                    "CREATE INDEX IF NOT EXISTS idx_socios_cnpj_cpf_socio ON socios(cnpj_cpf_socio)",
                ),
                ("Indexando socios por nome", "CREATE INDEX IF NOT EXISTS idx_socios_nome_socio ON socios(nome_socio)"),
                (
                    "Indexando socios por representante",
                    "CREATE INDEX IF NOT EXISTS idx_socios_representante ON socios(representante_legal)",
                ),
                (
                    "Indexando socios por nome do representante",
                    "CREATE INDEX IF NOT EXISTS idx_socios_representante_nome ON socios(nome_representante)",
                ),
            ]
        )

    tasks.extend(
        [
            (
                "Indexando simples",
                "CREATE INDEX IF NOT EXISTS idx_simples_cnpj_basico ON simples(cnpj_basico)",
            ),
            ("Criando tabela de referencia", "CREATE TABLE IF NOT EXISTS _referencia (referencia TEXT, valor TEXT)"),
        ]
    )
    return tasks
