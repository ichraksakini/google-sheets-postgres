import os
import json
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import gspread
import re
import hashlib
import time
from oauth2client.service_account import ServiceAccountCredentials

print("🔥 VERSION CORRIGÉE V4 🔥")

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

    # ================= HASH LIGNE =================
    def row_hash(values):
        """Génère un ID stable basé sur le contenu de la ligne — idempotent entre les runs."""
        content = json.dumps(values, ensure_ascii=False, sort_keys=False)
        return hashlib.md5(content.encode("utf-8")).hexdigest()

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

            # ================= COLONNES UNIQUES =================
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

            # ================= ADD COLUMNS — IF NOT EXISTS (FIX BUG 2) =================
            for col in columns:
                cursor.execute(
                    sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} TEXT")
                    .format(sql.Identifier(table_name), sql.Identifier(col))
                )
            conn.commit()
            print(f"✅ Colonnes vérifiées/ajoutées")

            # ================= PRÉPARER LES LIGNES =================
            batch = []

            for row in rows:
                values = [row[i] if i < len(row) else None for i in range(len(columns))]

                # ID stable basé sur le contenu (FIX idempotence)
                stable_id = row_hash(values)

                row_dict = {"id": stable_id}
                for col, val in zip(columns, values):
                    if col not in row_dict:   # garde le premier en cas de doublon résiduel
                        row_dict[col] = val

                batch.append(row_dict)

            # ================= BATCH INSERT (FIX performances) =================
            if batch:
                all_cols = list(batch[0].keys())
                cols_sql = sql.SQL(", ").join([sql.Identifier(c) for c in all_cols])
                placeholders = sql.SQL(", ").join([sql.Placeholder()] * len(all_cols))

                query = sql.SQL("""
                    INSERT INTO {table} ({cols})
                    VALUES ({placeholders})
                    ON CONFLICT (id) DO NOTHING
                """).format(
                    table=sql.Identifier(table_name),
                    cols=cols_sql,
                    placeholders=placeholders
                )

                inserted = 0
                errors = 0

                for row_dict in batch:
                    vals = [row_dict.get(c) for c in all_cols]
                    try:
                        cursor.execute(query, vals)
                        inserted += 1
                    except Exception as e:
                        conn.rollback()
                        errors += 1
                        print(f"⚠️ Ligne ignorée : {e}")

                conn.commit()   # FIX : commit APRÈS succès, pas avant
                print(f"✅ {table_name} terminé ({inserted} insertions, {errors} erreurs)")

        except Exception as e:
            conn.rollback()
            print(f"❌ Erreur {sheet_name} : {e}")

    cursor.close()
    conn.close()

    print("\n🎉 IMPORT TERMINÉ !")

except Exception as e:
    print("❌ ERREUR GLOBALE :", e)
    raise