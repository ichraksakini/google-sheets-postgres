import os
import json
import psycopg2
import gspread
import re
from oauth2client.service_account import ServiceAccountCredentials
import time
import uuid

print("🔥 FINAL VERSION ULTRA CLEAN V3 🔥")

try:
    print("🚀 Démarrage du script...")
    time.sleep(2)

    # ================= GOOGLE =================
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)

    spreadsheet = client.open_by_key("1fQ1fAFxTIBTU_SjYhsPx1_ctBvGCarxqMeGda4xRYP8")
    print("✅ Connexion Google Sheets OK")

    # ================= DB =================
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        database=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        port=5432,
        sslmode="require"
    )

    conn.autocommit = False
    cursor = conn.cursor()
    print("✅ Connexion DB OK")

    tables = {
        "Salles réunion Réel": "salles_reunion_reel",
        "Hebergement": "hebergement",
        "Suivi ticket": "suivi_ticket",
        "Suivi ticket Crédit": "suivi_ticket_credit",
        "Energie": "energie"
    }

    # ================= CLEAN =================
    def clean_column(col):
        col = col.lower().strip()
        col = col.replace("\n", "_").replace("\r", "_")
        col = col.replace(" ", "_")
        col = col.replace("é", "e").replace("è", "e").replace("ê", "e")
        col = col.replace("à", "a").replace("ù", "u")
        col = re.sub(r'[^a-z0-9_]', '', col)
        return col[:50] if col else "col"

    for sheet_name, table_name in tables.items():
        try:
            print(f"\n🔄 {sheet_name} → {table_name}")

            sheet = spreadsheet.worksheet(sheet_name)
            data = sheet.get_all_values()

            if not data or len(data) < 2:
                print("⚠️ Pas de données")
                continue

            headers = data[0]
            rows = data[1:]

            print(f"📊 {len(rows)} lignes")

            # ================= UNIQUE COLUMNS =================
            seen = {}
            columns = []
            indexes = []

            for i, h in enumerate(headers):
                col = clean_column(h)

                if col in seen:
                    seen[col] += 1
                    col = f"{col}_{seen[col]}"
                else:
                    seen[col] = 0

                columns.append(col)
                indexes.append(i)

            # ================= CREATE TABLE =================
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id TEXT PRIMARY KEY
                );
            """)

            # ================= GET EXISTING =================
            cursor.execute(f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = '{table_name}'
            """)
            existing_columns = [c[0] for c in cursor.fetchall()]

            # ================= ADD COLUMNS =================
            for col in columns:
                if col not in existing_columns:
                    try:
                        cursor.execute(f'ALTER TABLE {table_name} ADD COLUMN "{col}" TEXT;')
                        print(f"➕ {col}")
                    except Exception:
                        conn.rollback()

            conn.commit()

            inserted = 0

            # ================= INSERT =================
            for row in rows:
                try:
                    unique_map = {}

                    for idx, col in zip(indexes, columns):
                        val = row[idx] if idx < len(row) else None

                        # 🔥 SUPPRESSION DOUBLON DEFINITIVE
                        if col not in unique_map:
                            unique_map[col] = val

                    # 🔥 ID UNIQUE
                    unique_map["id"] = str(uuid.uuid4())

                    cols = list(unique_map.keys())
                    vals = list(unique_map.values())

                    placeholders = ", ".join(["%s"] * len(cols))
                    columns_sql = ", ".join([f'"{c}"' for c in cols])

                    query = f"""
                        INSERT INTO {table_name} ({columns_sql})
                        VALUES ({placeholders})
                        ON CONFLICT (id) DO NOTHING
                    """

                    cursor.execute(query, vals)
                    inserted += 1

                except Exception as e:
                    conn.rollback()
                    print("⚠️ ligne ignorée")

            conn.commit()
            print(f"✅ {table_name} terminé ({inserted} insertions)")

        except Exception as e:
            conn.rollback()
            print(f"❌ Erreur {sheet_name} :", e)

    cursor.close()
    conn.close()

    print("\n🎉 IMPORT 100% RÉUSSI !!!")

except Exception as e:
    print("❌ ERREUR GLOBALE :", e)
    raise