import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import time
from config import DB_CONFIG
import string
import os

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
BAD_ROWS_LOG = os.path.join(LOG_DIR, "bad_rows.log")


def log_event(conn, table, start_time, end_time, row_count, status, message=''):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO LOGS.etl_log(table_name, start_time, end_time, row_count, status, message)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (table, start_time, end_time, row_count, status, message))
        conn.commit()
    except Exception as e:
        print(f"⚠️ Ошибка при логировании: {e}")


def parse_dates(df):
    for col in df.columns:
        if "date" in col.lower():
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
                df[col] = df[col].apply(lambda x: x.date() if pd.notnull(x) else None)
            except Exception:
                pass
    return df


def read_csv_safe(csv_path):
    encodings = ['utf-8', 'cp1251', 'ISO-8859-1']
    for enc in encodings:
        try:
            return pd.read_csv(csv_path, encoding=enc, sep=';', engine='python')
        except UnicodeDecodeError:
            continue
    raise Exception("❌ Не удалось прочитать CSV в доступных кодировках.")


def convert_value(v):
    if pd.isnull(v):
        return None
    elif isinstance(v, (pd.Timestamp, np.datetime64)):
        return v.date()
    elif isinstance(v, (np.int64, np.int32, np.integer)):
        return int(v)
    elif isinstance(v, (np.float64, np.float32, np.floating)):
        return float(v)
    elif isinstance(v, (np.bool_)):
        return bool(v)
    else:
        return str(v).strip()


def log_bad_row(table_name, index, row_dict, reasons):
    with open(BAD_ROWS_LOG, "a", encoding="utf-8") as f:
        f.write(f"[{table_name}] ❌ Строка {index + 2}: {row_dict}\n   Причина: {', '.join(reasons)}\n")


def load_table(conn, table_name, csv_path, if_exists='upsert'):
    start = datetime.now()
    print(f"⏱️ Старт загрузки {table_name} в {start.strftime('%H:%M:%S')}")
    time.sleep(5)
    try:
        df = read_csv_safe(csv_path)
        df.columns = [col.lower() for col in df.columns]
        df = parse_dates(df)
        df = df.dropna(subset=[col for col in df.columns if 'date' in col.lower()])
        df = df.where(pd.notnull(df), None)

        if table_name == 'ds.md_exchange_rate_d':
            required = {'data_actual_date', 'currency_rk'}
            if not required.issubset(df.columns):
                raise Exception(f"Не хватает колонок в {table_name}: {df.columns}")
            df = df.drop_duplicates(subset=['data_actual_date', 'currency_rk'])

        if table_name == 'ds.md_currency_d':
            bad_rows = []
            for idx, row in df.iterrows():
                reasons = []

                code = row.get('currency_code')
                if pd.isnull(code) or not isinstance(code, (int, float)):
                    reasons.append(f"currency_code некорректен: {code}")

                iso = row.get('code_iso_char')
                if iso is None or pd.isnull(iso):
                    reasons.append("code_iso_char=None")
                else:
                    iso_str = ''.join(filter(str.isalpha, str(iso))).upper()
                    if len(iso_str) == 3:
                        df.at[idx, 'code_iso_char'] = iso_str
                    else:
                        reasons.append(f"code_iso_char некорректная: '{iso}' → '{iso_str}' (длина {len(iso_str)})")

                if reasons:
                    log_bad_row(table_name, idx, row.to_dict(), reasons)
                    bad_rows.append(idx)

            df = df.drop(index=bad_rows)
            print(f"⚠️ Отброшено {len(bad_rows)} строк из-за неверных значений валют (см. {BAD_ROWS_LOG})")

        with conn.cursor() as cur:
            if table_name == "ds.ft_posting_f":
                cur.execute("DELETE FROM ds.ft_posting_f")

            columns = list(df.columns)
            values = [tuple(convert_value(v) for v in row) for row in df.to_numpy()]

            def upsert_query(pk_cols):
                return f"""
                    INSERT INTO {table_name} ({', '.join(columns)})
                    VALUES %s
                    ON CONFLICT ({', '.join(pk_cols)})
                    DO UPDATE SET {', '.join([f'{col}=EXCLUDED.{col}' for col in columns if col not in pk_cols])}
                """

            if table_name == 'ds.ft_balance_f':
                query = upsert_query(['on_date', 'account_rk'])
            elif table_name == 'ds.md_account_d':
                query = upsert_query(['data_actual_date', 'account_rk'])
            elif table_name == 'ds.md_currency_d':
                query = upsert_query(['currency_rk', 'data_actual_date'])
            elif table_name == 'ds.md_exchange_rate_d':
                query = upsert_query(['data_actual_date', 'currency_rk'])
            elif table_name == 'ds.md_ledger_account_s':
                query = upsert_query(['ledger_account', 'start_date'])
            else:
                query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"

            try:
                execute_values(cur, query, values)
            except Exception as bulk_error:
                print(f"⚠️ Ошибка при массовой вставке в {table_name}: {bulk_error}")
                print("⏳ Переход на построчную загрузку...")
                failed_rows = 0
                for i, row in enumerate(values):
                    try:
                        row_query = query.replace("VALUES %s", "VALUES (" + ", ".join(["%s"] * len(row)) + ")")
                        cur.execute(row_query, row)
                    except Exception as row_error:
                        failed_rows += 1
                        print(f"❌ Строка {i + 1}: {row}\n   Причина: {row_error}")
                print(f"⏭️ Всего ошибок на строках: {failed_rows}")

        conn.commit()
        time.sleep(2)
        end = datetime.now()
        log_event(conn, table_name, start, end, len(df), 'SUCCESS')

    except Exception as e:
        conn.rollback()
        end = datetime.now()
        log_event(conn, table_name, start, end, 0, 'FAILURE', str(e))
        print(f"❌ Ошибка при загрузке {table_name}: {e}")


def main():
    conn = psycopg2.connect(**DB_CONFIG)

    files = {
        "ds.ft_balance_f": "data/ft_balance_f.csv",
        "ds.ft_posting_f": "data/ft_posting_f.csv",
        "ds.md_account_d": "data/md_account_d.csv",
        "ds.md_currency_d": "data/md_currency_d.csv",
        "ds.md_exchange_rate_d": "data/md_exchange_rate_d.csv",
        "ds.md_ledger_account_s": "data/md_ledger_account_s.csv"
    }

    for table, path in files.items():
        print(f"▶ Загрузка {table}...")
        load_table(conn, table, path, if_exists='upsert')

    conn.close()
    print("✅ Загрузка завершена.")


if __name__ == "__main__":
    main()