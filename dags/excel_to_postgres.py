from datetime import datetime
from pathlib import Path
import shutil
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# PATH DI DALAM CONTAINER (harus dimount ke host: ./data)
BASE_DIR = Path("/opt/airflow/data")
INCOMING_DIR = BASE_DIR / "incoming"
ARCHIVE_DIR = BASE_DIR / "archive"
ERROR_DIR = BASE_DIR / "error"

# Mapping kolom Excel -> kolom final
REQ_COLUMNS = {
    "id": "id",
    "name_sales_code": "salescode",
    "date_order": "dateorder",
    "amount_total": "totalsales",
    "brand": "brand",  # opsional
}

with DAG(
    dag_id="excel_to_postgres_daily",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # manual trigger
    catchup=False,
    tags=["excel", "postgres", "etl"],
):

    @task
    def list_excel_files() -> list[str]:
        """Scan folder incoming untuk .xlsx (bukan file sementara ~$.xlsx).
        FAIL jika tidak ada file, supaya terlihat jelas di UI.
        """
        INCOMING_DIR.mkdir(parents=True, exist_ok=True)
        ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        ERROR_DIR.mkdir(parents=True, exist_ok=True)

        files = []
        for pattern in ["*.xlsx", "*.XLSX"]:
            files += list(INCOMING_DIR.glob(pattern))
        files = [f for f in sorted(set(files)) if not f.name.startswith("~$")]
        print(f"FOUND FILES: {[str(f) for f in files]}")
        if not files:
            raise RuntimeError(
                f"Tidak ada file .xlsx di {INCOMING_DIR}. "
                "Pastikan volume sudah dimount dan file diletakkan di folder host ./data/incoming"
            )
        return [str(f) for f in files]

    @task
    def validate_and_transform(file_path: str) -> dict:
        """Baca Excel, validasi header, casting+clean, simpan CSV sementara.
        Bila gagal, file dipindahkan ke folder error.
        """
        p = Path(file_path)
        try:
            df = pd.read_excel(p, engine="openpyxl")

            # Validasi kolom minimal
            missing = [c for c in REQ_COLUMNS.keys() if c not in df.columns]
            if missing:
                raise ValueError(
                    f"Missing columns: {missing}. "
                    f"Ditemukan header: {list(df.columns)}"
                )

            # Subset + rename
            df = df[list(REQ_COLUMNS.keys())].rename(columns=REQ_COLUMNS)

            # Casting + clean
            df["id"] = pd.to_numeric(df["id"], errors="raise").astype("int64")
            df["salescode"] = df["salescode"].astype(str).str.strip()
            df["dateorder"] = pd.to_datetime(df["dateorder"], errors="coerce").dt.date
            df["totalsales"] = pd.to_numeric(df["totalsales"], errors="coerce").fillna(0.0)
            if "brand" in df.columns:
                df["brand"] = df["brand"].astype(str).str.strip()
            else:
                df["brand"] = None

            # Drop baris invalid
            df = df.dropna(subset=["id", "dateorder"])

            # Simpan CSV sementara di samping file asli
            tmp_csv = str(p.with_suffix(".csv"))
            df.to_csv(tmp_csv, index=False, encoding="utf-8")
            print(f"TRANSFORM OK: {p.name} -> {tmp_csv}, rows={len(df)}")
            return {"filepath": str(p), "tmp_csv": tmp_csv}

        except Exception as e:
            # Pindahkan file bermasalah ke error
            try:
                dest = ERROR_DIR / p.name
                shutil.move(str(p), dest)
                print(f"MOVED TO ERROR: {p.name} -> {dest}")
            except Exception:
                pass
            raise RuntimeError(f"Validasi/transform gagal untuk {p.name}: {e}") from e

    @task
    def load_upsert(payload: dict) -> None:
        """COPY CSV ke temp table, lalu UPSERT (ON CONFLICT) ke tabel final sales_daily.
        Setelah sukses, CSV dihapus + file asli diarsip ke archive.
        """
        file_path = payload["filepath"]
        tmp_csv = payload["tmp_csv"]

        hook = PostgresHook(postgres_conn_id="pg_dw")  # buat di UI: Admin > Connections
        conn = hook.get_conn()
        cur = conn.cursor()

        # Nama staging aman (huruf+angka)
        stem = Path(file_path).stem
        safe_stem = "".join(ch if (ch.isalnum() or ch == "_") else "_" for ch in stem.lower())
        staging_table = f"stg_sales_daily_{safe_stem}"

        cur.execute(
            f"""
            DROP TABLE IF EXISTS {staging_table};
            CREATE TEMP TABLE {staging_table} (
                id BIGINT,
                salescode TEXT,
                dateorder DATE,
                totalsales NUMERIC,
                brand TEXT
            ) ON COMMIT DROP;
            """
        )

        # COPY cepat
        with open(tmp_csv, "r", encoding="utf-8") as f:
            cur.copy_expert(
                f"COPY {staging_table} (id, salescode, dateorder, totalsales, brand) "
                f"FROM STDIN WITH CSV HEADER",
                f,
            )

        # UPSERT ke final (PK harus ada di sales_daily: PRIMARY KEY (id))
        cur.execute(
            f"""
            INSERT INTO sales_daily (id, salescode, dateorder, totalsales, brand)
            SELECT id, salescode, dateorder, totalsales, brand
            FROM {staging_table}
            ON CONFLICT (id) DO UPDATE SET
                salescode = EXCLUDED.salescode,
                dateorder = EXCLUDED.dateorder,
                totalsales = EXCLUDED.totalsales,
                brand = EXCLUDED.brand;
            """
        )

        conn.commit()
        cur.close()
        conn.close()

        # Cleanup + arsip
        Path(tmp_csv).unlink(missing_ok=True)
        shutil.move(file_path, ARCHIVE_DIR / Path(file_path).name)
        print(f"LOAD OK: {Path(file_path).name} diarsip ke {ARCHIVE_DIR}")

    # Orkestrasi
    files = list_excel_files()
    transformed = validate_and_transform.expand(file_path=files)  # dynamic task mapping
    load_upsert.expand(payload=transformed)
