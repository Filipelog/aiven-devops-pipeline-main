"""
Módulo compartilhado: conexão com o Postgres da Aiven e leitura da API
de dados abertos meteorológicos Open-Meteo (https://api.open-meteo.com).
Usado por app.py — concentra a lógica de acesso a dados num único módulo.
"""

import os
import datetime
from contextlib import contextmanager
import psycopg2
from psycopg2 import pool as psycopg2_pool
from psycopg2.extras import RealDictCursor
import requests

DB_URL = os.environ.get("AIVEN_DB_URL")
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# Coordenadas de Salvador, BA (pode alterar para qualquer cidade)
LATITUDE = -12.9714
LONGITUDE = -38.5014

# Séries meteorológicas mapeadas para a Open-Meteo — dados abertos, sem chave de API.
SERIES = {
    101: {"name": "Temperatura Máxima Diária (Salvador)", "unit": "°C", "casas": 1},
    102: {"name": "Temperatura Mínima Diária (Salvador)", "unit": "°C", "casas": 1},
    103: {"name": "Precipitação Acumulada (Salvador)", "unit": "mm", "casas": 1},
    104: {"name": "Velocidade Máxima do Vento (Salvador)", "unit": "km/h", "casas": 1},
}
POINTS_PER_SERIES = 30

_pool = None


def _get_pool():
    global _pool
    if _pool is None:
        if not DB_URL:
            raise RuntimeError("AIVEN_DB_URL não configurada")
        _pool = psycopg2_pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=5,
            dsn=DB_URL,
            sslmode="require",
            cursor_factory=RealDictCursor,
            connect_timeout=10,
        )
    return _pool


@contextmanager
def get_connection():
    """Pega uma conexão emprestada do pool e devolve ao sair do bloco `with`."""
    conn = _get_pool().getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        _get_pool().putconn(conn)


def init_db():
    """Cria as tabelas caso ainda não existam."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS meteo_series (
                        series_code INTEGER NOT NULL,
                        ref_date    DATE NOT NULL,
                        value       NUMERIC NOT NULL,
                        fetched_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                        PRIMARY KEY (series_code, ref_date)
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS visits (
                        id SERIAL PRIMARY KEY,
                        path TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                    """
                )
            conn.commit()
    except psycopg2.errors.DuplicateTable:
        pass
    except psycopg2.errors.UniqueViolation as exc:
        if "pg_type" not in str(exc):
            raise


def fetch_series_from_bcb(series_code: int, n: int = POINTS_PER_SERIES):
    """Busca os últimos N dias de dados climáticos reais na Open-Meteo."""
    mapa_campos = {
        101: "temperature_2m_max",
        102: "temperature_2m_min",
        103: "precipitation_sum",
        104: "wind_speed_10m_max",
    }

    if series_code not in mapa_campos:
        raise ValueError(f"Código de série não mapeado para Open-Meteo: {series_code}")

    campo_api = mapa_campos[series_code]

    params = {
    "latitude": LATITUDE,
    "longitude": LONGITUDE,
    "daily": campo_api,
    "past_days": n,
    "forecast_days": 1,
    "timezone": "auto",
}

    resp = requests.get(OPEN_METEO_URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    datas = data.get("daily", {}).get("time", [])
    valores = data.get("daily", {}).get(campo_api, [])

    parsed = []
    for data_str, val in zip(datas, valores):
        if val is not None:
            ref_date = datetime.datetime.strptime(data_str, "%Y-%m-%d").date()
            parsed.append({"ref_date": ref_date, "value": float(val)})

    return parsed[-n:]


def upsert_series(series_code: int, points: list):
    """Grava (ou atualiza) pontos de uma série direto no Postgres."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            for p in points:
                cur.execute(
                    """
                    INSERT INTO meteo_series (series_code, ref_date, value, fetched_at)
                    VALUES (%s, %s, %s, now())
                    ON CONFLICT (series_code, ref_date)
                    DO UPDATE SET value = EXCLUDED.value, fetched_at = now();
                    """,
                    (series_code, p["ref_date"], p["value"]),
                )
        conn.commit()