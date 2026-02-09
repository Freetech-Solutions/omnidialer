#!/usr/bin/env python3
import os
import sys
import json
import logging
import argparse
import gearman

# Intentamos psycopg (psycopg3) y si no, psycopg2
try:
    import psycopg  # type: ignore
    PSYCOPG3 = True
except Exception:
    PSYCOPG3 = False
    import psycopg2  # type: ignore  # noqa: F401


logging.basicConfig(
    level=os.getenv("LOGLEVEL", "INFO").upper(),
    format="%(asctime)s - %(levelname)s - %(message)s"
)

DEFAULT_QUERY = """
    SELECT
        c.id,
        COALESCE(c.telefono, c.phone, c.numero, '') AS tel,
        COALESCE(c.nombre, c.first_name, '') AS nombre,
        COALESCE(c.apellido, c.last_name, '') AS apellido
    FROM contact c
    JOIN contact_in_campaign cic
      ON cic.id_contact = c.id
    WHERE cic.id_campaign = %s
      AND COALESCE(cic.schedule_aborted, false) = false
      AND COALESCE(cic.finalized, false) = false
    ORDER BY cic.id_contact ASC
    LIMIT %s
"""


def get_db_dsn() -> str:
    dsn = os.getenv("DIALER_DB_DSN", "").strip()
    if dsn:
        return dsn

    host = (
        os.getenv("POSTGRES_DIALER_SERVER") or
        os.getenv("DIALER_DB_HOST") or
        "dialer-postgresql"
    ).strip()
    port = (
        os.getenv("POSTGRES_DIALER_PORT") or
        os.getenv("DIALER_DB_PORT") or
        "5432"
    ).strip()
    name = (
        os.getenv("POSTGRES_DIALER_DB") or
        os.getenv("DIALER_DB_NAME") or
        "omnidialer"
    ).strip()
    user = (
        os.getenv("POSTGRES_DIALER_USER") or
        os.getenv("DIALER_DB_USER") or
        "omnidialer"
    ).strip()
    pwd = (os.getenv("POSTGRES_DIALER_PASSWORD") or os.getenv("DIALER_DB_PASS") or "").strip()

    return f"host={host} port={port} dbname={name} user={user} password={pwd}"


def fetch_contacts_for_campaign(id_campaign: int, limit_n: int):
    dsn = get_db_dsn()

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            # --- 1) Detectar columnas reales en contact ---
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'contact'
            """)
            contact_cols = {r[0] for r in cur.fetchall()}

            phone_candidates = (
                "telefono", "phone", "numero", "phone_number", "tel", "mobile", "celular",
                "main_phone", "telefono1", "telefono_1", "telefono_principal"
            )
            phone_col = next((c for c in phone_candidates if c in contact_cols), None)
            if not phone_col:
                raise RuntimeError(
                    "No se encontró una columna de teléfono en 'contact'. "
                    f"Columnas disponibles: {sorted(contact_cols)}"
                )

            # --- 2) Detectar columnas reales en contact_in_campaign ---
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'contact_in_campaign'
            """)
            cic_cols = {r[0] for r in cur.fetchall()}

            # Armar filtros opcionales según columnas presentes
            cic_filters = ["cic.id_campaign = %s"]

            # schedule_aborted (si existe)
            if "schedule_aborted" in cic_cols:
                cic_filters.append("COALESCE(cic.schedule_aborted, false) = false")

            # finalized / is_finalized / completed / etc. (si existe)
            finalized_candidates = (
                "finalized", "is_finalized", "completed", "is_completed",
                "done", "finished"
            )
            finalized_col = next((c for c in finalized_candidates if c in cic_cols), None)
            if finalized_col:
                cic_filters.append(f"COALESCE(cic.{finalized_col}, false) = false")

            where_sql = " AND ".join(cic_filters)

            # --- 3) Query final ---
            query = f"""
                SELECT
                    c.id,
                    COALESCE(c.{phone_col}::text, '') AS tel
                FROM contact c
                JOIN contact_in_campaign cic
                  ON cic.id_contact = c.id
                WHERE {where_sql}
                ORDER BY cic.id_contact ASC
                LIMIT %s
            """

            cur.execute(query, (id_campaign, limit_n))
            return cur.fetchall()


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Encola jobs Gearman process-contact a partir de una campaña, "
            "extrayendo contactos desde DB."
        )
    )
    parser.add_argument("id_campaign", type=int, help="ID de campaña")
    parser.add_argument(
        "--n", type=int, default=1,
        help="Cantidad de contactos a encolar (default: 1)"
    )
    parser.add_argument(
        "--wait", action="store_true",
        help="Esperar a que el job termine y mostrar result"
    )
    parser.add_argument(
        "--gearman",
        default=os.getenv("GEARMAN_JOB_SERVERS", "gearman:4730"),
        help="GEARMAN_JOB_SERVERS (default: env o gearman:4730)"
    )
    args = parser.parse_args()

    rows = fetch_contacts_for_campaign(args.id_campaign, args.n)
    if not rows:
        logging.error(
            "No se encontraron contactos elegibles para campaña %s. "
            "Revisa CONTACT_QUERY/criterios.",
            args.id_campaign
        )
        sys.exit(2)

    client = gearman.GearmanClient([args.gearman])

    logging.info("-" * 60)
    logging.info(
        "Campaña=%s | Encolando %s contacto(s) a Gearman (%s)",
        args.id_campaign, len(rows), args.gearman
    )
    logging.info("-" * 60)

    for row in rows:

        row = list(row)

        contact_id = row[0]
        tel = row[1] if len(row) > 1 else ""

        # Garantiza índice 2
        contact = [contact_id, None, tel]

        payload = {"id_campaign": args.id_campaign, "contact": contact}
        data = json.dumps(payload).encode("utf-8")

        logging.info("Encolando process-contact para contact_id=%s", contact[0])

        job = client.submit_job(
            "process-contact",
            data,
            wait_until_complete=args.wait
        )

        if args.wait:
            logging.info(
                "Job state=%s result=%r",
                getattr(job, "state", None),
                getattr(job, "result", None)
            )


if __name__ == "__main__":
    main()
