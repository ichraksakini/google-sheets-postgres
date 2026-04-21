import os
import json
import psycopg2
from psycopg2 import sql
import gspread
import re
import hashlib
import time
from oauth2client.service_account import ServiceAccountCredentials

print("🔥 VERSION FINAL V6 MULTI-SHEETS 🔥")

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

    # ✅ MAIN SHEET (Mayssane réservations)
    spreadsheet_main = client.open_by_key("1fQ1fAFxTIBTU_SjYhsPx1_ctBvGCarxqMeGda4xRYP8")

    # ⚡ ENERGIE SHEET (SMARTFM)
   spreadsheet_energie = client.open_by_key("14Z772AyMOF72u6jNjpzXQN1Sj9aw2Cx6ahJrOCbobqA")

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

    # ================= CLEAN COL =================
    def clean_column(col):
        col = col.lower().strip()
        col = col.replace("\n", "_").replace("\r", "_")
        col = col.replace(" ", "_")
        col = col.replace("é", "e").replace("è", "e").replace("ê", "e")
        col = col.replace("à", "a").replace("ù", "u")
        col = re.sub(r'[^a-z0-9_]', '', col)
        return col[:50] if col else "col"

    # ================= HASH =================
    def row_hash(values):
        content = json.dumps(values, ensure_ascii=False)
        return hashlib.md5(content.encode()).hexdigest()

    # ================= PROCESS =================
    for sheet_name, table_name in tables.items():
        try:
            print(f"\n🔄 {sheet_name} → {table_name}")

            # 🔥 SWITCH AUTOMATIQUE GOOGLE SHEET
            if sheet_name == "Energie":
                sheet = spreadsheet_energie.worksheet(sheet_name)
            else:
                sheet = spreadsheet_main.worksheet(sheet_name)

            data = sheet.get_all_values()

            if not data or len(data) < 2:
                print("⚠️ Pas de données")
                continue

            headers = data[0]
            rows = data[1:]

            print(f"📊 {len(rows)} lignes")

            # 🔥 CLEAN + UNIQUE HEADERS
            seen = {}
            columns = []

            for h in headers:
                col = clean_column(h)

                if col in seen:
                    seen[col] += 1
                    col = f"{col}_{seen[col]}"
                else:
                    seen[col] = 0

                columns.append(col)

            # ================= CREATE TABLE =================
            cursor.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    id TEXT PRIMARY KEY
                );
            """).format(sql.Identifier(table_name)))

            conn.commit()

            # ================= ADD COLUMNS =================
            for col in columns:
                cursor.execute(
                    sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} TEXT")
                    .format(sql.Identifier(table_name), sql.Identifier(col))
                )

            conn.commit()
            print("✅ Colonnes OK")

            inserted = 0
            errors = 0

            # ================= INSERT =================
            for row in rows:
                try:
                    values = [row[i] if i < len(row) else None for i in range(len(columns))]

                    # 🔥 ID UNIQUE STABLE
                    stable_id = row_hash(values)

                    row_dict = {"id": stable_id}

                    for col, val in zip(columns, values):
                        if col not in row_dict:
                            row_dict[col] = val

                    unique_cols = list(dict.fromkeys(row_dict.keys()))
                    unique_vals = [row_dict[c] for c in unique_cols]

                    cols_sql = sql.SQL(", ").join(map(sql.Identifier, unique_cols))
                    placeholders = sql.SQL(", ").join(sql.Placeholder() * len(unique_cols))

                    query = sql.SQL("""
                        INSERT INTO {} ({})
                        VALUES ({})
                        ON CONFLICT (id) DO NOTHING
                    """).format(
                        sql.Identifier(table_name),
                        cols_sql,
                        placeholders
                    )

                    cursor.execute(query, unique_vals)
                    inserted += 1

                except Exception as e:
                    conn.rollback()
                    errors += 1
                    print("⚠️ ligne ignorée :", e)

            conn.commit()
            print(f"✅ {table_name} terminé ({inserted} insertions, {errors} erreurs)")

        except Exception as e:
            conn.rollback()
            print(f"❌ Erreur {sheet_name} :", e)

    cursor.close()
    conn.close()

    print("\n🎉 IMPORT 100% RÉUSSI !!!")

except Exception as e:
    print("❌ ERREUR GLOBALE :", e)
    raise