import os
import json
import psycopg2
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import time

print("🔥 VERSION FINALE NEON 🔥")

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
        os.environ["DATABASE_URL"]
    )

    cursor = conn.cursor()
    print("✅ Connexion NEON OK")

    # ================= FORMAT DATE =================
    def format_date(value):
        if not value:
            return None

        value = str(value).strip()

        formats = [
            "%m/%d/%Y %H:%M:%S",
            "%m/%d/%Y",
            "%d/%m/%Y",
            "%Y-%m-%d"
        ]

        for fmt in formats:
            try:
                return datetime.strptime(value, fmt)
            except:
                continue

        return None

    # ================= TABLES =================
    tables = {
        "Salles réunion Réel": "salles_reunion_reel",
        "Hebergement": "hebergement",
        "Suivi ticket": "suivi_ticket",
        "Suivi ticket Crédit": "suivi_ticket_credit",
        "Energie": "energie"
    }

    # ================= TRAITEMENT =================
    for sheet_name, table_name in tables.items():
        try:
            print(f"\n🔄 {sheet_name} → {table_name}")

            sheet = spreadsheet.worksheet(sheet_name)
            data = sheet.get_all_values()

            if not data:
                print("⚠️ Aucun data")
                continue

            headers = data[0]
            rows = data[1:]

            print(f"📊 {len(rows)} lignes")

            # 🔥 transformer noms colonnes
            columns = []
            for h in headers:
                col = h.lower().strip()
                col = col.replace(" ", "_").replace("é", "e").replace("è", "e")
                col = col.replace("à", "a").replace("/", "_")
                col = col.replace("'", "")
                columns.append(col)

            # ================= CREATE TABLE =================
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY
                );
            """)

            # ================= ADD MISSING COLUMNS =================
            cursor.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
            """)
            existing_columns = [col[0] for col in cursor.fetchall()]

            for col in columns:
                if col not in existing_columns:
                    try:
                        cursor.execute(f"""
                            ALTER TABLE {table_name}
                            ADD COLUMN {col} TEXT;
                        """)
                        print(f"➕ colonne ajoutée: {col}")
                    except Exception as e:
                        print(f"⚠️ erreur ajout colonne {col}: {e}")

            conn.commit()

            inserted = 0

            # ================= INSERT DATA =================
            for row in rows:
                values = []
                valid_columns = []

                for i, col in enumerate(columns):
                    val = row[i] if i < len(row) else None

                    # format date auto
                    if "date" in col:
                        val = format_date(val)

                    valid_columns.append(col)
                    values.append(val)

                placeholders = ", ".join(["%s"] * len(valid_columns))
                columns_sql = ", ".join(valid_columns)

                query = f"""
                    INSERT INTO {table_name} ({columns_sql})
                    VALUES ({placeholders})
                """

                cursor.execute(query, values)
                inserted += 1

            conn.commit()
            print(f"✅ {table_name} terminé ({inserted} lignes insérées)")

        except Exception as e:
            print(f"❌ Erreur {sheet_name} :", e)

    cursor.close()
    conn.close()

    print("\n🎉 TOUT EST IMPORTÉ AVEC SUCCÈS !!!")

except Exception as e:
    print("❌ ERREUR GLOBALE :", e)
    raise