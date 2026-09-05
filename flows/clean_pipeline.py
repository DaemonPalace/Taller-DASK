"""
clean_pipeline.py
Prefect flow que orquesta la limpieza distribuida del dataset sucio
sobre el clúster Dask (1 scheduler + 3 workers).
"""

import glob
import os
import re

import dask
import pandas as pd
from dask.distributed import Client
import dask.dataframe as dd
from prefect import flow, task, get_run_logger

SCHEDULER_ADDRESS = os.environ.get("DASK_SCHEDULER_ADDRESS", "tcp://dask-scheduler:8786")
RAW_DIR = os.environ.get("RAW_DIR", "shared-data/raw")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "shared-data/processed")
WORKER_NAMES = ["worker-1", "worker-2", "worker-3"]

ANOMALY_TOKEN = "CUST-00000-ANOMALY"
CODE_RE = re.compile(r"(\d{5})")
NULL_LIKE_PHONES = {"DESCONOCIDO", "N/A", "--", "", "NAN"}


# ---------------------------------------------------------------------------
# Funciones de limpieza (ejecutadas por partición en cada worker)
# ---------------------------------------------------------------------------

def fix_mojibake(text):
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return None
    text = str(text)
    if text.strip() == "":
        return None
    try:
        repaired = text.encode("latin1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        repaired = text
    # el generador sintetico usa un guion ASCII literal en vez del byte real
    # de mojibake para 'i' con tilde, asi que no revierte via encode/decode
    repaired = repaired.replace("Ã-", "í")
    return repaired


def clean_customer_code(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ANOMALY_TOKEN
    text = str(value).strip()
    if text == "":
        return ANOMALY_TOKEN
    match = CODE_RE.search(text)
    if not match:
        return ANOMALY_TOKEN
    return f"CUST-{match.group(1)}"


def clean_phone(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip().upper()
    if text in NULL_LIKE_PHONES:
        return None
    digits = re.sub(r"\D", "", text)
    # quitar prefijo pais 57 / 0057 dejando numero local de 10 digitos
    if digits.startswith("0057"):
        digits = digits[4:]
    elif digits.startswith("57") and len(digits) > 10:
        digits = digits[2:]
    if len(digits) != 10:
        return None
    return f"+57{digits}"


def clean_partition(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.copy()
    pdf["customer_code_clean"] = pdf["raw_customer_code"].map(clean_customer_code)
    pdf["city_notes_clean"] = pdf["city_notes_corrupted"].map(fix_mojibake)
    pdf["phone_clean"] = pdf["phone_raw"].map(clean_phone)
    return pdf


def chunk_evenly(items: list, n: int) -> list:
    k, m = divmod(len(items), n)
    return [items[i * k + min(i, m): (i + 1) * k + min(i + 1, m)] for i in range(n)]


# ---------------------------------------------------------------------------
# Tasks de Prefect
# ---------------------------------------------------------------------------

@task(retries=3, retry_delay_seconds=5)
def validate_infrastructure(scheduler_address: str) -> str:
    logger = get_run_logger()
    client = Client(scheduler_address, timeout="10s")
    try:
        n_workers = len(client.scheduler_info()["workers"])
        logger.info(f"Cluster activo: {n_workers} worker(s) conectados en {scheduler_address}")
        if n_workers < 1:
            raise RuntimeError("Ningun worker Dask disponible en el cluster.")
        return scheduler_address
    finally:
        client.close()


@task
def validate_raw_data(raw_dir: str) -> list:
    logger = get_run_logger()
    files = sorted(glob.glob(os.path.join(raw_dir, "*.csv")))
    if not files:
        raise FileNotFoundError(f"No se encontraron CSV crudos en {raw_dir}")
    logger.info(f"Encontrados {len(files)} archivos particionados en {raw_dir}")
    return files


@task(retries=1, retry_delay_seconds=5)
def ingest_on_worker(scheduler_address: str, worker_name: str, files: list, output_dir: str) -> str:
    """Etapa 1/2: lee los CSV crudos asignados y los materializa como parquet
    intermedio, todo anclado al worker Dask 'worker_name' via
    dask.annotate(workers=...). Dask sigue troceando cada CSV en particiones
    out-of-core acotadas por blocksize (no un pandas gigante en memoria)."""
    logger = get_run_logger()
    client = Client(scheduler_address)
    try:
        logger.info(f"[{worker_name}] ingiriendo {len(files)} archivo(s): {[os.path.basename(f) for f in files]}")
        os.makedirs(output_dir, exist_ok=True)
        out_path = os.path.join(output_dir, f"raw_ingested_{worker_name}.parquet")

        with dask.annotate(workers=worker_name, allow_other_workers=False):
            ddf = dd.read_csv(
                files,
                blocksize="16MB",
                dtype={
                    "raw_customer_code": "object",
                    "city_notes_corrupted": "object",
                    "phone_raw": "object",
                },
            )
            ddf.to_parquet(out_path, engine="pyarrow", write_index=False, overwrite=True)

        logger.info(f"[{worker_name}] ingest -> {out_path}")
        return out_path
    finally:
        client.close()


@task(retries=1, retry_delay_seconds=5)
def clean_on_worker(scheduler_address: str, worker_name: str, raw_parquet_path: str, output_dir: str) -> str:
    """Etapa 2/2: aplica la limpieza (mojibake, regex de codigo, telefono) sobre
    el parquet intermedio, sin salir del worker que lo ingirio."""
    logger = get_run_logger()
    client = Client(scheduler_address)
    try:
        out_path = os.path.join(output_dir, f"transactions_clean_{worker_name}.parquet")

        with dask.annotate(workers=worker_name, allow_other_workers=False):
            ddf = dd.read_parquet(raw_parquet_path)
            meta = ddf._meta.assign(
                customer_code_clean="",
                city_notes_clean="",
                phone_clean="",
            )
            cleaned = ddf.map_partitions(clean_partition, meta=meta)
            cleaned.to_parquet(out_path, engine="pyarrow", write_index=False, overwrite=True)

        logger.info(f"[{worker_name}] clean -> {out_path}")
        return out_path
    finally:
        client.close()


@task
def quality_gate(scheduler_address: str, parquet_dirs: list, anomaly_threshold: float = 0.15) -> dict:
    logger = get_run_logger()
    part_files = sorted(
        f for d in parquet_dirs for f in glob.glob(os.path.join(d, "*.parquet"))
    )
    client = Client(scheduler_address)
    try:
        ddf = dd.read_parquet(part_files)
        total = ddf.shape[0].compute()
        anomalies = (ddf["customer_code_clean"] == ANOMALY_TOKEN).sum().compute()
        null_phones = ddf["phone_clean"].isna().sum().compute()
    finally:
        client.close()

    anomaly_rate = anomalies / total if total else 1.0
    logger.info(
        f"Quality gate: total={total:,} anomalies={anomalies:,} "
        f"({anomaly_rate:.2%}) null_phones={null_phones:,}"
    )

    if anomaly_rate > anomaly_threshold:
        raise AssertionError(
            f"Tasa de anomalias {anomaly_rate:.2%} supera el umbral {anomaly_threshold:.0%}"
        )

    return {
        "total_rows": int(total),
        "anomalies": int(anomalies),
        "anomaly_rate": anomaly_rate,
        "null_phones": int(null_phones),
    }


# ---------------------------------------------------------------------------
# Flow principal
# ---------------------------------------------------------------------------

@flow(name="dask-dirty-data-cleaning")
def cleaning_flow(
    scheduler_address: str = SCHEDULER_ADDRESS,
    raw_dir: str = RAW_DIR,
    output_dir: str = OUTPUT_DIR,
):
    logger = get_run_logger()
    logger.info("Iniciando pipeline de limpieza distribuida")

    active_scheduler = validate_infrastructure(scheduler_address)
    files = validate_raw_data(raw_dir)

    groups = [g for g in chunk_evenly(files, len(WORKER_NAMES)) if g]

    clean_futures = []
    for worker_name, group in zip(WORKER_NAMES, groups):
        ingest_future = ingest_on_worker.with_options(
            name=f"ingest-{worker_name}", tags=[worker_name]
        ).submit(active_scheduler, worker_name, group, output_dir)

        clean_future = clean_on_worker.with_options(
            name=f"clean-{worker_name}", tags=[worker_name]
        ).submit(active_scheduler, worker_name, ingest_future, output_dir)

        clean_futures.append(clean_future)

    parquet_paths = [f.result() for f in clean_futures]

    metrics = quality_gate(active_scheduler, parquet_paths)

    logger.info(f"Pipeline finalizado. Metricas: {metrics}")
    return metrics


if __name__ == "__main__":
    cleaning_flow()
