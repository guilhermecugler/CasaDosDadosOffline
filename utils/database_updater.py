from __future__ import annotations

import io
import os
import re
import shutil
import sqlite3
import time
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from threading import Event
from typing import Callable, Dict, Iterator, List, Optional, Sequence, Tuple

import pandas as pd
import requests

SHARE_TOKEN = "YggdBLfdninEJX9"
DAV_BASE_URL = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/{SHARE_TOKEN}"
MIRROR_BASE_URL = "https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/"
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) CasaDosDadosOffline/1.0",
}
DAV_NAMESPACES = {"d": "DAV:"}

CONNECT_TIMEOUT = 20
READ_TIMEOUT = 120
STREAM_CHUNK = 4 * 1024 * 1024
CSV_CHUNK_ROWS = 100000

REFERENCE_FILENAME = ".receita_reference"

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


def _fmt_speed(bps: float) -> str:
    if bps >= 1024 ** 2:
        return f"{bps / 1024 ** 2:.1f} MB/s"
    if bps >= 1024:
        return f"{bps / 1024:.1f} KB/s"
    return f"{bps:.0f} B/s"


def _fmt_size(b: int) -> str:
    if b >= 1024 ** 3:
        return f"{b / 1024 ** 3:.1f} GB"
    if b >= 1024 ** 2:
        return f"{b / 1024 ** 2:.1f} MB"
    if b >= 1024:
        return f"{b / 1024:.1f} KB"
    return f"{b} B"


def _fmt_eta(seconds: float) -> str:
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m{s % 60:02d}s"
    return f"{s // 3600}h{(s % 3600) // 60:02d}m"


def update_cnpj_database(
    status_callback: StatusCallback,
    progress_callback: ProgressCallback = None,
    cancel_event: Optional[Event] = None,
    cleanup: bool = False,
    source: str = "auto",
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
        reference, remote_modified, remote_files = _fetch_remote_dataset_info(status_callback, source)
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
        expected_suffixes = _build_expected_suffix_map(categories)
        stored_reference = _read_zip_reference(zip_dir)
        skip_existing_downloads = stored_reference in (None, reference)
        _remove_partial_downloads(zip_dir)

        downloaded = _download_remote_files(
            remote_files,
            zip_dir,
            status_callback,
            progress,
            cancel_event,
            skip_existing=skip_existing_downloads,
            expected_suffixes=expected_suffixes,
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
        else:
            _write_zip_reference(zip_dir, reference)

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



def _remove_partial_downloads(directory: Path) -> None:
    for entry in directory.glob('*.part'):
        try:
            entry.unlink()
        except OSError:
            continue


def _read_zip_reference(zip_dir: Path) -> Optional[str]:
    reference_path = zip_dir / REFERENCE_FILENAME
    if not reference_path.exists():
        return None
    try:
        value = reference_path.read_text(encoding='utf-8').strip()
    except OSError:
        return None
    return value or None


def _write_zip_reference(zip_dir: Path, reference: str) -> None:
    reference_path = zip_dir / REFERENCE_FILENAME
    try:
        reference_path.write_text(reference, encoding='utf-8')
    except OSError:
        return


def _build_expected_suffix_map(categories: Dict[str, object]) -> Dict[str, Tuple[str, ...]]:
    mapping: Dict[str, Tuple[str, ...]] = {}

    for name in categories.get('empresas', []):
        mapping[str(name).lower()] = EMPRESAS_SUFFIXES
    for name in categories.get('estabelecimentos', []):
        mapping[str(name).lower()] = ESTABELECIMENTO_SUFFIXES
    for name in categories.get('socios', []):
        mapping[str(name).lower()] = SOCIOS_SUFFIXES
    for name in categories.get('simples', []):
        mapping[str(name).lower()] = SIMPLES_SUFFIXES

    code_tables: Dict[str, Optional[str]] = categories.get('code_tables', {})  # type: ignore[assignment]
    if isinstance(code_tables, dict):
        for config in CODE_TABLES:
            zip_name = code_tables.get(config['table'])
            if zip_name:
                mapping[str(zip_name).lower()] = config['suffixes']
    return mapping


def _zip_has_expected_content(path: Path, suffixes: Optional[Tuple[str, ...]]) -> bool:
    if not path.exists():
        return False
    try:
        if path.stat().st_size == 0:
            return False
    except OSError:
        return False
    if not zipfile.is_zipfile(path):
        return False
    try:
        with zipfile.ZipFile(path) as archive:
            if suffixes:
                try:
                    _resolve_csv_member(archive, suffixes)
                except DatabaseUpdateError:
                    return False
        return True
    except (OSError, zipfile.BadZipFile):
        return False


def _propfind(url: str, depth: str = "1") -> requests.Response:
    response = requests.request(
        "PROPFIND",
        url,
        headers={**HTTP_HEADERS, "Depth": depth, "Content-Type": "application/xml"},
        timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
    )
    response.raise_for_status()
    return response


def _parse_dav_entries(xml_text: str) -> List[Tuple[str, bool, Optional[datetime]]]:
    """Return list of (name, is_collection, last_modified) from a PROPFIND response."""
    root = ET.fromstring(xml_text)
    entries: List[Tuple[str, bool, Optional[datetime]]] = []
    for resp in root.findall("d:response", DAV_NAMESPACES):
        href = resp.findtext("d:href", default="", namespaces=DAV_NAMESPACES)
        name = href.rstrip("/").rsplit("/", 1)[-1]
        if not name:
            continue
        is_col = resp.find("d:propstat/d:prop/d:resourcetype/d:collection", DAV_NAMESPACES) is not None
        lm_raw = resp.findtext("d:propstat/d:prop/d:getlastmodified", default="", namespaces=DAV_NAMESPACES)
        last_modified: Optional[datetime] = None
        if lm_raw:
            try:
                last_modified = parsedate_to_datetime(lm_raw).replace(tzinfo=None)
            except Exception:
                pass
        entries.append((name, is_col, last_modified))
    return entries


def _fetch_remote_dataset_info(
    status_callback: Optional[StatusCallback] = None,
    source: str = "auto",
) -> Tuple[str, Optional[datetime], List[Tuple[str, str]]]:
    if source == "mirror":
        reference_full, modified = _fetch_latest_mirror_reference()
        files = _fetch_mirror_files(reference_full)
        return "-".join(reference_full.split("-")[:2]), modified, files

    if source == "receita":
        reference, modified = _fetch_latest_remote_reference()
        files = _fetch_dataset_files(reference)
        return reference, modified, files

    # auto: tenta RF primeiro, cai no espelho se falhar
    try:
        reference, modified = _fetch_latest_remote_reference()
        files = _fetch_dataset_files(reference)
        return reference, modified, files
    except (requests.RequestException, DatabaseUpdateError) as primary_exc:
        if status_callback:
            status_callback("Receita Federal inacessivel, usando espelho Casa dos Dados...")
        try:
            reference_full, modified = _fetch_latest_mirror_reference()
            files = _fetch_mirror_files(reference_full)
            return "-".join(reference_full.split("-")[:2]), modified, files
        except (requests.RequestException, DatabaseUpdateError) as mirror_exc:
            raise DatabaseUpdateError(
                f"Falha ao acessar a Receita Federal ({primary_exc}) "
                f"e o espelho Casa dos Dados ({mirror_exc})."
            ) from mirror_exc


def _fetch_latest_mirror_reference() -> Tuple[str, Optional[datetime]]:
    response = requests.get(
        MIRROR_BASE_URL, headers=HTTP_HEADERS, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
    )
    response.raise_for_status()
    folders = re.findall(r'href="(\d{4}-\d{2}-\d{2}/)"', response.text)
    if not folders:
        raise DatabaseUpdateError(
            "Nao foi possivel identificar a ultima versao no espelho Casa dos Dados."
        )
    folders.sort(reverse=True)
    return folders[0].rstrip("/"), None


def _fetch_mirror_files(reference: str) -> List[Tuple[str, str]]:
    url = f"{MIRROR_BASE_URL}{reference}/"
    response = requests.get(url, headers=HTTP_HEADERS, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    response.raise_for_status()
    names = re.findall(r'href="([^"]+\.zip)"', response.text, re.IGNORECASE)
    if not names:
        raise DatabaseUpdateError(f"Nenhum arquivo zip encontrado no espelho ({reference}).")
    files = [(name, f"{url}{name}") for name in names]
    files.sort(key=lambda item: item[0].lower())
    return files


def _fetch_latest_remote_reference() -> Tuple[str, Optional[datetime]]:
    response = _propfind(f"{DAV_BASE_URL}/")
    entries = _parse_dav_entries(response.text)
    folder_pattern = re.compile(r"^\d{4}-\d{2}$")
    folders = [(name, lm) for name, is_col, lm in entries if is_col and folder_pattern.match(name)]
    if not folders:
        raise DatabaseUpdateError(
            "Nao foi possivel identificar a ultima versao da base na Receita Federal."
        )
    folders.sort(key=lambda x: x[0], reverse=True)
    return folders[0]


def _fetch_dataset_files(reference: str) -> List[Tuple[str, str]]:
    response = _propfind(f"{DAV_BASE_URL}/{reference}/")
    entries = _parse_dav_entries(response.text)
    files: List[Tuple[str, str]] = []
    for name, is_col, _ in entries:
        if not is_col and name.lower().endswith(".zip"):
            files.append((name, f"{DAV_BASE_URL}/{reference}/{name}"))
    if not files:
        raise DatabaseUpdateError(f"Nenhum arquivo zip encontrado em {reference}.")
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
    skip_existing: bool,
    expected_suffixes: Optional[Dict[str, Tuple[str, ...]]] = None,
) -> List[Tuple[str, Path]]:
    downloaded: List[Tuple[str, Path]] = []
    total = len(remote_files)
    for index, (name, url) in enumerate(remote_files, start=1):
        _check_cancel(cancel_event)
        local_path = destination / name
        suffixes = expected_suffixes.get(name.lower()) if expected_suffixes else None

        reuse_existing = False
        if skip_existing and local_path.exists():
            if _zip_has_expected_content(local_path, suffixes):
                status_callback(f"Reutilizando {name} ({index}/{total})...")
                reuse_existing = True
            else:
                status_callback(f"Arquivo local invalido, baixando {name} ({index}/{total})...")
        else:
            status_callback(f"Baixando {name} ({index}/{total})...")

        if not reuse_existing:
            _stream_download(url, local_path, cancel_event, status_callback, f"{name} ({index}/{total})")

        downloaded.append((name, local_path))
        progress.increment()
    return downloaded


def _stream_download(
    url: str,
    destination: Path,
    cancel_event: Optional[Event],
    status_callback: Optional[StatusCallback] = None,
    file_label: str = "",
) -> None:
    temporary = destination.with_suffix(destination.suffix + ".part")
    if temporary.exists():
        temporary.unlink()

    try:
        with requests.get(url, headers=HTTP_HEADERS, stream=True, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)) as response:
            response.raise_for_status()
            raw_cl = response.headers.get("Content-Length", "")
            content_length: Optional[int] = int(raw_cl) if raw_cl.isdigit() else None
            downloaded_bytes = 0
            start_time = time.monotonic()
            last_report = start_time

            with open(temporary, "wb") as handler:
                for chunk in response.iter_content(STREAM_CHUNK):
                    _check_cancel(cancel_event)
                    if not chunk:
                        continue
                    handler.write(chunk)
                    downloaded_bytes += len(chunk)

                    now = time.monotonic()
                    if status_callback and file_label and (now - last_report) >= 0.5:
                        elapsed = now - start_time
                        speed = downloaded_bytes / elapsed if elapsed > 0 else 0
                        parts = [f"Baixando {file_label}"]
                        if speed > 0:
                            parts.append(_fmt_speed(speed))
                        if content_length:
                            parts.append(f"{_fmt_size(downloaded_bytes)} / {_fmt_size(content_length)}")
                            remaining = content_length - downloaded_bytes
                            if speed > 0 and remaining > 0:
                                parts.append(f"ETA {_fmt_eta(remaining / speed)}")
                        status_callback(" | ".join(parts))
                        last_report = now

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
    patterns = [
        re.compile(rf"{re.escape(suffix)}(?:\.[^/\\]+)*$")
        for suffix in suffixes_lower
    ]

    for member in archive.namelist():
        member_lower = member.lower()
        for suffix, pattern in zip(suffixes_lower, patterns):
            if member_lower.endswith(suffix) or pattern.search(member_lower):
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
