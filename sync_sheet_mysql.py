import os
import json
import psycopg2
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import time

try:
    print("🚀 Démarrage du script...")

    # 🔥 petit délai pour stabilité GitHub
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

    # ================= SUPABASE =================
    conn = psycopg2.connect(
    host=os.environ["DB_HOST"],
    database=os.environ["DB_NAME"],
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
    port=5432,  # ✅ IMPORTANT
    sslmode="require"
)

    cursor = conn.cursor()
    print("✅ Connexion Supabase OK")

    # ================= DATE FIX =================
    def format_date(value):
        if value is None:
            return None

        value = str(value).strip()
        if value == "":
            return None

        formats = [
            "%m/%d/%Y %H:%M:%S",
            "%m/%d/%Y",
            "%d/%m/%Y",
            "%Y-%m-%d"
        ]

        for fmt in formats:
            try:
                return datetime.strptime(value, fmt).strftime("%Y-%m-%d %H:%M:%S")
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

            # 🔥 récupérer colonnes PostgreSQL
            cursor.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
            """)
            pg_columns = [col[0] for col in cursor.fetchall()]

            # 🔥 nettoyage colonnes
            columns = []
            for h in headers:
                col = h.lower().strip()
                col = col.replace(" ", "_").replace("é", "e").replace("è", "e")
                col = col.replace("à", "a").replace("/", "_")
                columns.append(col)

            inserted = 0

            for row in rows:
                values = []
                valid_columns = []

                for i, col in enumerate(columns):
                    if col not in pg_columns:
                        continue

                    val = row[i] if i < len(row) else None

                    # 🔥 gestion date auto
                    if any(k in col for k in ["date", "time"]):
                        val = format_date(val)

                    valid_columns.append(col)
                    values.append(val)

                if not values:
                    continue

                placeholders = ", ".join(["%s"] * len(valid_columns))
                columns_sql = ", ".join(valid_columns)

                query = f"""
                    INSERT INTO {table_name} ({columns_sql})
                    VALUES ({placeholders})
                    ON CONFLICT DO NOTHING
                """

                cursor.execute(query, values)
                inserted += 1

            conn.commit()
            print(f"✅ {table_name} terminé ({inserted} insertions)")

        except Exception as e:
            print(f"❌ Erreur {sheet_name} :", e)

    cursor.close()
    conn.close()

    print("\n🎉 Synchronisation terminée avec succès !")

except Exception as e:
    print("❌ ERREUR GLOBALE :", e)
    raise