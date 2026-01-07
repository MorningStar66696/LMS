from flask import Flask, render_template, request
import psycopg2
from dotenv import load_dotenv
import os
from flask import Response
import csv
from io import StringIO
import requests
from flask import jsonify
from flask import session, redirect
app = Flask(__name__)
app.secret_key = "crm-secret-key"

# Load env vars
load_dotenv()

DB_USER = os.getenv("SUPABASE_DB_USER")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
DB_HOST = os.getenv("SUPABASE_DB_HOST")
DB_PORT = os.getenv("SUPABASE_DB_PORT")
DB_NAME = os.getenv("SUPABASE_DB_NAME")
GOOGLE_SCRIPT_URL = "https://script.google.com/macros/s/AKfycby-PdJWriOifyJHWBqf7YIKZiKq7nQ1AnQD-o8oRd0pVVDxs1OIH4m3kqmlig5nOeaM/exec"
def get_connection():
    return psycopg2.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=int(DB_PORT),
        dbname=DB_NAME
    )
def get_table_columns(cur, table_name):
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
    """, (table_name,))
    return cur.fetchall()

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email = request.form.get("email")
        password = request.form.get("password")

        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT id, email, password_hash, role
            FROM users
            WHERE email = %s AND is_active = TRUE
        """, (email,))

        user = cur.fetchone()
        cur.close()
        conn.close()

        if not user:
            return render_template("login.html", error="Invalid credentials")

        user_id, email, db_password, role = user

        # ✅ PLAIN TEXT COMPARISON
        if password != db_password:
            return render_template("login.html", error="Invalid credentials")

        # ✅ LOGIN SUCCESS
        session["user_id"] = user_id
        session["user_email"] = email
        session["user_role"] = role

        return redirect("/")

    return render_template("login.html")


    

@app.route("/analytics")
def analytics():
    if "user_id" not in session:
        return redirect("/login")

    return render_template("analytics.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")







@app.route("/download")
def download():
    search = request.args.get("search")

    where_clauses = []
    params = []

    # 🔍 Global search
    if search:
        where_clauses.append("""
            EXISTS (
                SELECT 1
                FROM jsonb_each_text(to_jsonb("Main"))
                WHERE value ILIKE %s
            )
        """)
        params.append(f"%{search}%")

    # 🔥 SAME FILTER LOGIC
    for key in request.args:
        if key.startswith("filter_"):
            column = key.replace("filter_", "")
            values = request.args.getlist(key)
            if values:
                sub = []
                for v in values:
                    sub.append(f"\"{column}\"::text ILIKE %s")
                    params.append(f"%{v}%")
                where_clauses.append("(" + " OR ".join(sub) + ")")

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    query = f"""
        SELECT *
        FROM "Main"
        {where_sql}
        ORDER BY "created_time" DESC
    """

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query, params)

    rows = cur.fetchall()
    headers = [desc[0] for desc in cur.description]

    # 🔽 CREATE CSV
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(headers)
    writer.writerows(rows)

    cur.close()
    conn.close()

    filename = "leads_filtered.csv" if where_sql else "leads_all.csv"

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )


@app.route("/send-to-sheet", methods=["POST"])
def send_to_sheet():
    import json

    payload = request.json

    ctids = payload["ctids"]
    mappings = payload["mappings"]
    sheet_url = payload["sheetUrl"]

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM "Main"
        WHERE ctid = ANY(%s::tid[])
    """, (ctids,))

    rows = cur.fetchall()
    headers = [desc[0] for desc in cur.description]

    data = [dict(zip(headers, r)) for r in rows]

    cur.close()
    conn.close()

    # 🔹 BUILD MAPPED ROWS
    mapped_rows = []

    for row in data:
        obj = {}
        for m in mappings:
            table_key = m["tableField"].strip().upper()
            sheet_key = m["sheetField"].strip().lower()

            normalized_row = {
                k.strip().upper(): v
                for k, v in row.items()
            }

            obj[sheet_key] = normalized_row.get(table_key)

        mapped_rows.append(obj)

    # 🔴 🔴 🔴 DEBUG LOGS (THIS IS WHAT YOU ASKED)
    print("\n===== SEND TO SHEET DEBUG =====")

    print("=== CTIDS ===")
    print(ctids)

    print("=== MAPPINGS ===")
    print(json.dumps(mappings, indent=2))

    print("=== SAMPLE DB ROW ===")
    print(data[0] if data else "NO DATA")

    print("=== MAPPED ROW ===")
    print(json.dumps(mapped_rows[0] if mapped_rows else {}, indent=2))

    print("===== END DEBUG =====\n")

    # 🔹 SEND TO APPS SCRIPT
    r = requests.post(
        GOOGLE_SCRIPT_URL,
        json={
            "sheetUrl": sheet_url,
            "rows": mapped_rows,
            "mappings": mappings   # 🔥 REQUIRED
        },
        timeout=60
    )

    return jsonify({"message": "Data sent to Google Sheet successfully"})







@app.route("/download-selected")
def download_selected():
    ctids = request.args.getlist("ids")

    if not ctids:
        return "No rows selected", 400

    conn = get_connection()
    cur = conn.cursor()

    query = """
        SELECT *
        FROM "Main"
        WHERE ctid = ANY(%s::tid[])
        ORDER BY "created_time" DESC
    """

    cur.execute(query, (ctids,))

    rows = cur.fetchall()
    headers = [desc[0] for desc in cur.description]

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(headers)
    writer.writerows(rows)

    cur.close()
    conn.close()

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=leads_selected.csv"
        }
    )
    


@app.route("/api/analytics")
def analytics_data():
    conn = get_connection()
    cur = conn.cursor()

    # ✅ Leads by Month
    cur.execute("""
        SELECT
            TO_CHAR(TO_DATE("Date",'DD/MM/YYYY'), 'Mon YYYY') AS month,
            COUNT(*)
        FROM "Main"
        WHERE "Date" ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
        GROUP BY month, DATE_TRUNC('month', TO_DATE("Date",'DD/MM/YYYY'))
        ORDER BY DATE_TRUNC('month', TO_DATE("Date",'DD/MM/YYYY'))
    """)
    leads_by_month = cur.fetchall()

    # ✅ Top Courses (Overall)
    cur.execute("""
        SELECT "Course", COUNT(*)
        FROM "Main"
        WHERE "Course" IS NOT NULL AND TRIM("Course") <> ''
        GROUP BY "Course"
        ORDER BY COUNT(*) DESC
        LIMIT 10
    """)
    top_courses = cur.fetchall()

    # ✅ Top Courses (This Month)
    cur.execute("""
        SELECT "Course", COUNT(*)
        FROM "Main"
        WHERE "Course" IS NOT NULL
          AND "Date" ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
          AND DATE_TRUNC('month', TO_DATE("Date",'DD/MM/YYYY')) =
              DATE_TRUNC('month', CURRENT_DATE)
        GROUP BY "Course"
        ORDER BY COUNT(*) DESC
        LIMIT 5
    """)
    top_courses_month = cur.fetchall()

    # ✅ Lead Sources
    cur.execute("""
        SELECT "Source", COUNT(*)
        FROM "Main"
        WHERE "Source" IS NOT NULL
        GROUP BY "Source"
        ORDER BY COUNT(*) DESC
        LIMIT 8
    """)
    sources = cur.fetchall()

    # ✅ Top Campaigns
    cur.execute("""
        SELECT "Campaign Name", COUNT(*)
        FROM "Main"
        WHERE "Campaign Name" IS NOT NULL
        GROUP BY "Campaign Name"
        ORDER BY COUNT(*) DESC
        LIMIT 8
    """)
    top_campaigns = cur.fetchall()

    # ✅ Top Ad Sets
    cur.execute("""
        SELECT "Ad-set Name", COUNT(*)
        FROM "Main"
        WHERE "Ad-set Name" IS NOT NULL
        GROUP BY "Ad-set Name"
        ORDER BY COUNT(*) DESC
        LIMIT 8
    """)
    top_adsets = cur.fetchall()

        # 🔥 Insight 1: Top Course This Month
    cur.execute("""
        SELECT "Course", COUNT(*)
        FROM "Main"
        WHERE "Course" IS NOT NULL
          AND "Date" ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
          AND DATE_TRUNC('month', TO_DATE("Date",'DD/MM/YYYY')) =
              DATE_TRUNC('month', CURRENT_DATE)
        GROUP BY "Course"
        ORDER BY COUNT(*) DESC
        LIMIT 1
    """)
    top_course_month = cur.fetchone()


    # 📣 Insight 2: Top Source vs Second Source
    cur.execute("""
        SELECT "Source", COUNT(*)
        FROM "Main"
        WHERE "Source" IS NOT NULL
        GROUP BY "Source"
        ORDER BY COUNT(*) DESC
        LIMIT 2
    """)
    top_sources_compare = cur.fetchall()


    # 🌍 Insight 3: Country dominance for top course
    top_course_name = top_course_month[0] if top_course_month else None

    country_for_course = None
    if top_course_name:
        cur.execute("""
            SELECT "Target Country", COUNT(*)
            FROM "Main"
            WHERE "Course" = %s
              AND "Target Country" IS NOT NULL
            GROUP BY "Target Country"
            ORDER BY COUNT(*) DESC
            LIMIT 1
        """, (top_course_name,))
        country_for_course = cur.fetchone()

    cur.close()
    conn.close()

    return {
        "leads_by_month": leads_by_month,
        "top_courses": top_courses,
        "top_courses_month": top_courses_month,
        "sources": sources,
        "top_campaigns": top_campaigns,
        "top_adsets": top_adsets,

        # 🔥 Insights
        "insights": {
            "top_course_month": top_course_month,
            "top_sources_compare": top_sources_compare,
            "country_for_course": country_for_course
        }
    }
    
@app.route("/")
def index():
    if "user_id" not in session:
        return redirect("/login")
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 100))
    offset = (page - 1) * limit

    search = request.args.get("search")

    where_clauses = []
    params = []

    # 🔍 Global search
    if search:
        where_clauses.append("""
            EXISTS (
                SELECT 1
                FROM jsonb_each_text(to_jsonb("Main"))
                WHERE value ILIKE %s
            )
        """)
        params.append(f"%{search}%")

    # 🔥 MULTI-SELECT COLUMN FILTERS
    selected_filters = {}
    for key in request.args:
        if key.startswith("filter_"):
            column = key.replace("filter_", "")
            values = request.args.getlist(key)
            if values:
                selected_filters[column] = values
                sub = []
                for v in values:
                    sub.append(f"\"{column}\" ILIKE %s")
                    params.append(f"%{v}%")
                where_clauses.append("(" + " OR ".join(sub) + ")")

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    query_data = f"""
    SELECT
    ROW_NUMBER() OVER (ORDER BY "created_time" DESC) AS "rownum",
ctid AS "__ctid",
        "Date"                         AS "DATE",
        "Time"                         AS "TIME",
        "Full Name"                    AS "FULL NAME",
        "Phone Number"                 AS "PHONE NUMBER",
        "Email"                        AS "EMAIL",
        "City"                         AS "CITY",
        "State"                        AS "STATE",
        "Course_City"                  AS "COURSE_CITY",
        "Course_State"                 AS "COURSE_STATE",
        "Course"                       AS "COURSE",
        "Target Country"               AS "TARGET COUNTRY",
        "Intake Year"                  AS "INTAKE YEAR",
        "Target College Name"          AS "TARGET COLLEGE NAME",
        "Target College State"         AS "TARGET COLLEGE STATE",
        "Source"                       AS "SOURCE",
        "created_time"                 AS "CREATED_TIME",
        "Ad-set Name"                  AS "AD-SET NAME",
        "Ad-set ID"                    AS "AD-SET ID",
        "Form Name"                    AS "FORM NAME",
        "Campaign Name"                AS "CAMPAIGN NAME",
        "Number_Course"                AS "NUMBER_COURSE",
        "Mode"                         AS "MODE",
        "Form Id"                      AS "FORM ID",
        "Database Creation Date"       AS "DATABASE CREATION DATE",
        "Database Creation Time"       AS "DATABASE CREATION TIME",
        "Number_Course 2"              AS "NUMBER_COURSE 2",
        "Spreadsheet Source"           AS "SPREADSHEET SOURCE"
    FROM "Main"
    {where_sql}
    ORDER BY "created_time" DESC
    LIMIT %s OFFSET %s
"""

    query_count = f"""
        SELECT COUNT(*)
        FROM "Main"
        {where_sql}
    """

    conn = get_connection()
    cur = conn.cursor()

    # Fetch table data
    cur.execute(query_data, params + [limit, offset])
    rows = cur.fetchall()
    headers = [desc[0] for desc in cur.description]

    # Count
    cur.execute(query_count, params)
    total = cur.fetchone()[0]


    # 🔁 Build WHERE clause for cascading filters
    filter_where_clauses = []
    filter_params = []

    for col, values in selected_filters.items():
        sub = []
        for v in values:
            sub.append(f"\"{col}\"::text ILIKE %s")
            filter_params.append(f"%{v}%")
        filter_where_clauses.append("(" + " OR ".join(sub) + ")")

    base_filter_where_sql = ""
    if filter_where_clauses:
        base_filter_where_sql = "WHERE " + " AND ".join(filter_where_clauses)
    # 🔥 FETCH DISTINCT FILTER VALUES
# 🔥 FETCH DISTINCT FILTER VALUES (TYPE SAFE)
# 🔥 FETCH ALL COLUMNS DYNAMICALLY
    columns = get_table_columns(cur, "Main")

    filter_options = {}

    for col, data_type in columns:

        if data_type in ("json", "jsonb", "bytea"):
            continue

        # ❗ exclude current column from applied filters
        other_filters = []
        other_params = []

        for f_col, f_vals in selected_filters.items():
            if f_col == col:
                continue
            sub = []
            for v in f_vals:
                sub.append(f"\"{f_col}\"::text ILIKE %s")
                other_params.append(f"%{v}%")
            other_filters.append("(" + " OR ".join(sub) + ")")

        other_where_sql = ""
        if other_filters:
            other_where_sql = "WHERE " + " AND ".join(other_filters)

        # TEXT columns
        if data_type in ("character varying", "text", "character"):
            cur.execute(f"""
                SELECT DISTINCT "{col}"
                FROM "Main"
                {other_where_sql}
                {"AND" if other_where_sql else "WHERE"} "{col}" IS NOT NULL
                AND TRIM("{col}"::text) <> ''
                ORDER BY "{col}"::text
                LIMIT 500
            """, other_params)
            filter_options[col] = [r[0] for r in cur.fetchall()]

        # NUMERIC
        elif data_type in ("integer", "bigint", "numeric", "smallint"):
            cur.execute(f"""
                SELECT DISTINCT "{col}"
                FROM "Main"
                {other_where_sql}
                {"AND" if other_where_sql else "WHERE"} "{col}" IS NOT NULL
                ORDER BY "{col}"
                LIMIT 500
            """, other_params)
            filter_options[col] = [str(r[0]) for r in cur.fetchall()]

        # DATE / TIMESTAMP
        elif data_type.startswith("timestamp") or data_type == "date":
            cur.execute(f"""
                SELECT DISTINCT "{col}"::text
                FROM "Main"
                {other_where_sql}
                {"AND" if other_where_sql else "WHERE"} "{col}" IS NOT NULL
                ORDER BY "{col}" DESC
                LIMIT 200
            """, other_params)
            filter_options[col] = [r[0] for r in cur.fetchall()]

    cur.close()
    conn.close()

    data = [dict(zip(headers, row)) for row in rows]

    return render_template(
        "index.html",
        headers=headers,
        data=data,
        page=page,
        total=total,
        limit=limit,
        search=search or "",
        filter_options=filter_options,
        selected_filters=selected_filters
    )

if __name__ == "__main__":
    app.run()