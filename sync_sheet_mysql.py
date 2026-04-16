import os
import json
import psycopg2
import gspread
import re
from oauth2client.service_account import ServiceAccountCredentials
import time

print("🔥 FINAL VERSION FIXED 💯")

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

    # ================= NEON =================
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        database=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        port=5432,
        sslmode="require"
    )

    cursor = conn.cursor()
    print("✅ Connexion NEON OK")

    tables = {
        "Salles réunion Réel": "salles_reunion_reel",
        "Hebergement": "hebergement",
        "Suivi ticket": "suivi_ticket",
        "Suivi ticket Crédit": "suivi_ticket_credit",
        "Energie": "energie"
    }

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

            # ================= CLEAN COLUMNS =================
            seen = {}
            columns = []

            for h in headers:
                col = h.lower().strip()

                col = col.replace("\n", "_").replace("\r", "_")
                col = col.replace(" ", "_")
                col = col.replace("é", "e").replace("è", "e").replace("ê", "e")
                col = col.replace("à", "a").replace("ù", "u")

                col = re.sub(r'[^a-z0-9_]', '', col)

                if not col:
                    col = "col"

                col = col[:50]

                if col in seen:
                    seen[col] += 1
                    col = f"{col}_{seen[col]}"
                else:
                    seen[col] = 1

                columns.append(col)

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
            existing_columns = [col[0] for col in cursor.fetchall()]

            # ================= ADD COLUMNS =================
            for col in columns:
                if col not in existing_columns:
                    try:
                        cursor.execute(f'''
                            ALTER TABLE {table_name}
                            ADD COLUMN "{col}" TEXT;
                        ''')
                        print(f"➕ colonne ajoutée: {col}")
                    except:
                        print(f"⚠️ colonne ignorée: {col}")

            conn.commit()

            inserted = 0

            # ================= INSERT =================
            for row in rows:
                try:
                    values = []
                    valid_columns = []

                    for i, col in enumerate(columns):
                        val = row[i] if i < len(row) else None
                        valid_columns.append(col)
                        values.append(val)

                    record_id = values[0] if values[0] else str(hash(str(values)))

                    valid_columns.insert(0, "id")
                    values.insert(0, record_id)

                    placeholders = ", ".join(["%s"] * len(valid_columns))
                    columns_sql = ", ".join([f'"{c}"' for c in valid_columns])

                    query = f"""
                        INSERT INTO {table_name} ({columns_sql})
                        VALUES ({placeholders})
                        ON CONFLICT (id) DO NOTHING
                    """

                    cursor.execute(query, values)
                    inserted += 1

                except Exception:
                    conn.rollback()
                    print("⚠️ ligne ignorée")

            conn.commit()
            print(f"✅ {table_name} terminé ({inserted} insertions)")

        except Exception as e:
            conn.rollback()
            print(f"❌ Erreur {sheet_name} :", e)

    cursor.close()
    conn.close()

    print("\n🎉 IMPORT COMPLET RÉUSSI !!!")

except Exception as e:
    print("❌ ERREUR GLOBALE :", e)
    raise