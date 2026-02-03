# from flask import Flask, render_template, request
# import psycopg2
# from dotenv import load_dotenv
# import os
# from flask import Response
# import csv
# from io import StringIO
# import requests
# from flask import jsonify
# from flask import session, redirect
# app = Flask(__name__)
# app.secret_key = "crm-secret-key"
# from apscheduler.schedulers.background import BackgroundScheduler
# import json
# from datetime import date


# DB_USER = os.getenv("SUPABASE_DB_USER")
# DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
# DB_HOST = os.getenv("SUPABASE_DB_HOST")
# DB_PORT = os.getenv("SUPABASE_DB_PORT", "5432")
# DB_NAME = os.getenv("SUPABASE_DB_NAME")
# GOOGLE_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbxw6Xp-fPNhMBirx7wWcrXd4vYGXFrbJxB9OJK_zjRjAwb1w4SyAr9KLWotUxn3fuft/exec"
# def get_connection():
#     missing = []
#     if not DB_HOST: missing.append("SUPABASE_DB_HOST")
#     if not DB_USER: missing.append("SUPABASE_DB_USER")
#     if not DB_PASSWORD: missing.append("SUPABASE_DB_PASSWORD")
#     if not DB_NAME: missing.append("SUPABASE_DB_NAME")

#     if missing:
#         raise Exception(f"Missing env vars: {', '.join(missing)}")

#     return psycopg2.connect(
#         host=DB_HOST,
#         user=DB_USER,
#         password=DB_PASSWORD,
#         dbname=DB_NAME,
#         port=int(DB_PORT),
#         connect_timeout=5
#     )
# def get_table_columns(cur, table_name):
#     cur.execute("""
#         SELECT column_name, data_type
#         FROM information_schema.columns
#         WHERE table_name = %s
#         ORDER BY ordinal_position
#     """, (table_name,))
#     return cur.fetchall()

# @app.route("/login", methods=["GET", "POST"])
# def login():
#     if request.method == "POST":
#         email = request.form.get("email")
#         password = request.form.get("password")

#         conn = get_connection()
#         cur = conn.cursor()

#         cur.execute("""
#             SELECT id, email, password_hash, role
#             FROM users
#             WHERE email = %s AND is_active = TRUE
#         """, (email,))

#         user = cur.fetchone()
#         cur.close()
#         conn.close()

#         if not user:
#             return render_template("login.html", error="Invalid credentials")

#         user_id, email, db_password, role = user

#         # ✅ PLAIN TEXT COMPARISON
#         if password != db_password:
#             return render_template("login.html", error="Invalid credentials")

#         # ✅ LOGIN SUCCESS
#         session["user_id"] = user_id
#         session["user_email"] = email
#         session["user_role"] = role

#         return redirect("/")

#     return render_template("login.html")


    

# @app.route("/analytics")
# def analytics():
#     if "user_id" not in session:
#         return redirect("/login")

#     return render_template("analytics.html")

# @app.route("/logout")
# def logout():
#     session.clear()
#     return redirect("/login")







# @app.route("/download")
# def download():
#     search = request.args.get("search")

#     where_clauses = []
#     params = []

#     # 🔍 Global search
#     if search:
#         where_clauses.append("""
#             EXISTS (
#                 SELECT 1
#                 FROM jsonb_each_text(to_jsonb("Main"))
#                 WHERE value ILIKE %s
#             )
#         """)
#         params.append(f"%{search}%")

#     # 🔥 SAME FILTER LOGIC
#     for key in request.args:
#         if key.startswith("filter_"):
#             column = key.replace("filter_", "")
#             values = request.args.getlist(key)
#             if values:
#                 sub = []
#                 for v in values:
#                     sub.append(f"\"{column}\"::text ILIKE %s")
#                     params.append(f"%{v}%")
#                 where_clauses.append("(" + " OR ".join(sub) + ")")

#     where_sql = ""
#     if where_clauses:
#         where_sql = "WHERE " + " AND ".join(where_clauses)

#     query = f"""
#         SELECT *
#         FROM "Main"
#         {where_sql}
#         ORDER BY "created_time" DESC
#     """

#     conn = get_connection()
#     cur = conn.cursor()
#     cur.execute(query, params)

#     rows = cur.fetchall()
#     headers = [desc[0] for desc in cur.description]

#     # 🔽 CREATE CSV
#     output = StringIO()
#     writer = csv.writer(output)
#     writer.writerow(headers)
#     writer.writerows(rows)

#     cur.close()
#     conn.close()

#     filename = "leads_filtered.csv" if where_sql else "leads_all.csv"

#     return Response(
#         output.getvalue(),
#         mimetype="text/csv",
#         headers={
#             "Content-Disposition": f"attachment; filename={filename}"
#         }
#     )
# @app.route("/save-settings", methods=["POST"])
# def save_settings():
#     data = request.json

#     name = data.get("name")
#     mappings = data.get("mappings", [])
#     sheet_url = data.get("sheetUrl")
#     rules = data.get("automationRules", [])
#     switch_rules = data.get("switchRules", [])
#     trigger_interval = data.get("triggerInterval")
#     daily_ro = data.get("dailyRo")

#     # Convert daily RO
#     if daily_ro in ("", None, "null"):
#         daily_ro = None
#     else:
#         daily_ro = int(daily_ro)

#     conn = get_connection()
#     cur = conn.cursor()

#     cur.execute("""
#         INSERT INTO saved_settings
#         (user_id, name, sheet_url, mappings, automation_rules, switch_rules,
#          trigger_interval, daily_ro, created_by, created_at)
#         VALUES (%s, %s, %s, %s, %s, %s,
#                 %s, %s, %s, NOW())
#         RETURNING id
#     """, (
#         session["user_id"],
#         name,
#         sheet_url,
#         json.dumps(mappings),
#         json.dumps(rules),
#         json.dumps(switch_rules),
#         trigger_interval,
#         daily_ro,
#         session["user_email"]
#     ))

#     new_id = cur.fetchone()[0]
#     conn.commit()
#     cur.close()
#     conn.close()

#     return jsonify({"message": "Saved successfully", "id": new_id})

# @app.route("/saved-campaigns")
# def saved_campaigns():
#     if "user_id" not in session:
#         return redirect("/login")

#     conn = get_connection()
#     cur = conn.cursor()

#     cur.execute("""
#        SELECT id, name, query_string, sheet_url, trigger_interval, created_at, created_by
#         FROM saved_settings
#         WHERE user_id = %s
#         ORDER BY id DESC
#     """, (session["user_id"],))

#     rows = cur.fetchall()
#     cur.close()
#     conn.close()

#     return render_template("saved_campaigns.html", campaigns=rows)



# @app.route("/saved-campaigns/<int:id>")
# def saved_campaign_detail(id):
#     if "user_id" not in session:
#         return redirect("/login")

#     conn = get_connection()
#     cur = conn.cursor()

#     cur.execute("""
# SELECT id, name, query_string, mappings,
#        sheet_url, trigger_interval,
#        automation_rules, daily_ro,
#        updated_by, updated_at, last_trigger_run,
#        created_by, created_at
# FROM saved_settings
# WHERE id = %s AND user_id = %s
#     """, (id, session["user_id"]))

#     row = cur.fetchone()

#     cur.close()
#     conn.close()

#     if not row:
#         return "Campaign not found", 404

#     return render_template("saved_campaign_detail.html", campaign=row)




# @app.route("/saved-campaigns/delete/<int:id>", methods=["POST"])
# def delete_campaign(id):
#     conn = get_connection()
#     cur = conn.cursor()

#     cur.execute("""
#         DELETE FROM saved_settings
#         WHERE id = %s AND user_id = %s
#     """, (id, session["user_id"]))

#     conn.commit()
#     cur.close()
#     conn.close()

#     # 🔥 Delete trigger if running
#     job_id = f"job_{id}"
#     try:
#         scheduler.remove_job(job_id)
#         print(f"🛑 Deleted trigger {job_id} because campaign was deleted by user")
#     except Exception:
#         pass

#     return redirect("/saved-campaigns")


# @app.route("/saved-campaigns/edit/<int:id>", methods=["GET", "POST"])
# def edit_campaign(id):
#     if request.method == "POST":
#         name = request.form.get("name")
#         sheet_url = request.form.get("sheet_url")
#         interval = request.form.get("interval")

#         mappings = json.loads(request.form.get("mappings_json") or "[]")
#         rules = json.loads(request.form.get("rules_json") or "[]")
#         switch_rules = json.loads(request.form.get("switch_json") or "[]")

#         daily_ro = request.form.get("daily_ro")
#         if daily_ro in ("", None, "null"):
#             daily_ro = None
#         else:
#             daily_ro = int(daily_ro)

#         conn = get_connection()
#         cur = conn.cursor()

#         cur.execute("""
#         UPDATE saved_settings
#         SET name = %s,
#             sheet_url = %s,
#             trigger_interval = %s,
#             mappings = %s,
#             automation_rules = %s,
#             daily_ro = %s,
#             updated_by = %s,
#             updated_at = NOW(),
#             switch_rules = %s
#         WHERE id = %s AND user_id = %s
#     """, (
#         name,
#         sheet_url,
#         interval,
#         json.dumps(mappings),
#         json.dumps(rules),
#         daily_ro,                    # ✔ correct
#         session["user_email"],
#         json.dumps(switch_rules),    # ✔ correct
#         id,
#         session["user_id"]
#     ))

#         conn.commit()
#         cur.close()
#         conn.close()

#         return redirect(f"/saved-campaigns/{id}")

#     # ---------------------------
#     # GET — Load existing campaign
#     # ---------------------------

#     conn = get_connection()
#     cur = conn.cursor()

#     cur.execute("""
#         SELECT id, name, query_string, mappings,
#        sheet_url, trigger_interval,
#        automation_rules, daily_ro,
#        updated_by, updated_at, last_trigger_run,
#        created_by, created_at
# FROM saved_settings
#         WHERE id = %s AND user_id = %s
#     """, (id, session["user_id"]))

#     row = cur.fetchone()

#     # Load table columns
#     columns = get_table_columns(cur, "Main")
#     table_columns = [col for col, dtype in columns]

#     cur.close()
#     conn.close()

#     if not row:
#         return "Campaign not found", 404

#     # ---------------------------
#     # SAFE JSONB LOADING FIX
#     # ---------------------------

#     raw_mappings = row[3]
#     raw_rules = row[6]

#     mappings = raw_mappings if isinstance(raw_mappings, list) else json.loads(raw_mappings or "[]")
#     rules = raw_rules if isinstance(raw_rules, list) else json.loads(raw_rules or "[]")
#     raw_switch = row[13]

#     switch_rules = raw_switch if isinstance(raw_switch, list) else json.loads(raw_switch or "[]")

#     return render_template(
#         "edit_campaign_full.html",
#         campaign=row,
#         mappings_json=mappings,
#         rules_json=rules,
#         switch_json=switch_rules,
#         table_columns=table_columns
#     )





# @app.route("/start-trigger", methods=["POST"])
# def start_trigger():
#     data = request.json
#     setting_id = data["id"]
#     interval = int(data["interval"])

#     scheduler.add_job(
#         func=auto_send_job,
#         trigger='interval',
#         minutes=interval,
#         args=[setting_id],
#         id=f"job_{setting_id}",
#         replace_existing=True
#     )

#     return jsonify({"message": f"Trigger set every {interval} minutes"})




# @app.route("/send-to-sheet", methods=["POST"])
# def send_to_sheet():
#     payload = request.json

#     ctids = payload.get("ctids", [])
#     mappings = payload.get("mappings", [])
#     switch_rules = payload.get("switchRules", [])
#     manual_rules = payload.get("manualRules", [])
#     sheet_url = payload["sheetUrl"]

#     conn = get_connection()
#     cur = conn.cursor()

#     # ----------------------------------
#     # 1️⃣ MANUAL RULE FILTER MODE
#     # ----------------------------------
#     if manual_rules and len(manual_rules) > 0:
#         extra_conditions = []
#         params = []

#         for rule in manual_rules:
#             field = rule["field"].title()
#             values = [v.strip().lower() for v in rule["values"]]

#             placeholders = ", ".join(["%s"] * len(values))
#             extra_conditions.append(
#                 f'LOWER(TRIM("{field}"::text)) IN ({placeholders})'
#             )
#             params.extend(values)

#         where_sql = " AND ".join(extra_conditions)

#         cur.execute(f"""
#             SELECT *
#             FROM "Main"
#             WHERE {where_sql}
#             ORDER BY created_time DESC
#         """, params)

#     # ----------------------------------
#     # 2️⃣ NORMAL MODE (Selected rows)
#     # ----------------------------------
#     else:
#         if not ctids:
#             return jsonify({"success": False, "error": "Select rows OR add Manual Rules"}), 400

#         cur.execute("""
#             SELECT *
#             FROM "Main"
#             WHERE ctid = ANY(%s::tid[])
#         """, (ctids,))

#     rows = cur.fetchall()
#     headers = [desc[0] for desc in cur.description]
#     cur.close()
#     conn.close()

#     # Convert rows
#     data = [dict(zip(headers, r)) for r in rows]

#     # ----------------------------------
#     # 3️⃣ BUILD MAPPED ROWS
#     # ----------------------------------
#     mapped_rows = []

#     for row in data:
#         norm = {str(k).strip().upper(): v for k, v in row.items()}
#         mapped = {}

#         for m in mappings:
#             db_col = m["tableField"].strip().upper()
#             sheet_col = m["sheetField"].strip()
#             static_val = m.get("staticValue", "").strip()

#             if static_val:
#                 mapped[sheet_col] = static_val
#             else:
#                 mapped[sheet_col] = norm.get(db_col, "")

#         # Apply switch rules
#         for rule in switch_rules:
#             field = rule["field"].upper()
#             old_val = str(rule["old_value"]).lower().strip()
#             new_val = rule["new_value"]

#             db_value = str(norm.get(field, "")).lower().strip()
#             if db_value == old_val:
#                 for m in mappings:
#                     if m["tableField"].strip().upper() == field:
#                         col = m["sheetField"]
#                         mapped[col] = new_val
#                         break

#         mapped_rows.append(mapped)

#     # ----------------------------------
#     # 4️⃣ SEND TO GOOGLE SHEET
#     # ----------------------------------
#     try:
#         r = requests.post(
#             GOOGLE_SCRIPT_URL,
#             json={
#                 "sheetUrl": sheet_url,
#                 "rows": mapped_rows,
#                 "mappings": mappings
#             },
#             timeout=60
#         )

#         gs_response = r.json()

#         return jsonify({
#             "success": True,
#             "sent": len(mapped_rows),
#             "gs_response": gs_response
#         })

#     except Exception as e:
#         return jsonify({
#             "success": False,
#             "error": str(e)
#         }), 500







# @app.route("/download-selected")
# def download_selected():
#     ctids = request.args.getlist("ids")

#     if not ctids:
#         return "No rows selected", 400

#     conn = get_connection()
#     cur = conn.cursor()

#     query = """
#         SELECT *
#         FROM "Main"
#         WHERE ctid = ANY(%s::tid[])
#         ORDER BY "created_time" DESC
#     """

#     cur.execute(query, (ctids,))

#     rows = cur.fetchall()
#     headers = [desc[0] for desc in cur.description]

#     output = StringIO()
#     writer = csv.writer(output)
#     writer.writerow(headers)
#     writer.writerows(rows)

#     cur.close()
#     conn.close()

#     return Response(
#         output.getvalue(),
#         mimetype="text/csv",
#         headers={
#             "Content-Disposition": "attachment; filename=leads_selected.csv"
#         }
#     )
    


# @app.route("/api/analytics")
# def analytics_data():
#     conn = get_connection()
#     cur = conn.cursor()

#     # ✅ Leads by Month
#     cur.execute("""
#         SELECT
#             TO_CHAR(TO_DATE("Date",'DD/MM/YYYY'), 'Mon YYYY') AS month,
#             COUNT(*)
#         FROM "Main"
#         WHERE "Date" ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
#         GROUP BY month, DATE_TRUNC('month', TO_DATE("Date",'DD/MM/YYYY'))
#         ORDER BY DATE_TRUNC('month', TO_DATE("Date",'DD/MM/YYYY'))
#     """)
#     leads_by_month = cur.fetchall()

#     # ✅ Top Courses (Overall)
#     cur.execute("""
#         SELECT "Course", COUNT(*)
#         FROM "Main"
#         WHERE "Course" IS NOT NULL AND TRIM("Course") <> ''
#         GROUP BY "Course"
#         ORDER BY COUNT(*) DESC
#         LIMIT 10
#     """)
#     top_courses = cur.fetchall()

#     # ✅ Top Courses (This Month)
#     cur.execute("""
#         SELECT "Course", COUNT(*)
#         FROM "Main"
#         WHERE "Course" IS NOT NULL
#           AND "Date" ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
#           AND DATE_TRUNC('month', TO_DATE("Date",'DD/MM/YYYY')) =
#               DATE_TRUNC('month', CURRENT_DATE)
#         GROUP BY "Course"
#         ORDER BY COUNT(*) DESC
#         LIMIT 5
#     """)
#     top_courses_month = cur.fetchall()

#     # ✅ Lead Sources
#     cur.execute("""
#         SELECT "Source", COUNT(*)
#         FROM "Main"
#         WHERE "Source" IS NOT NULL
#         GROUP BY "Source"
#         ORDER BY COUNT(*) DESC
#         LIMIT 8
#     """)
#     sources = cur.fetchall()

#     # ✅ Top Campaigns
#     cur.execute("""
#         SELECT "Campaign Name", COUNT(*)
#         FROM "Main"
#         WHERE "Campaign Name" IS NOT NULL
#         GROUP BY "Campaign Name"
#         ORDER BY COUNT(*) DESC
#         LIMIT 8
#     """)
#     top_campaigns = cur.fetchall()

#     # ✅ Top Ad Sets
#     cur.execute("""
#         SELECT "Ad-set Name", COUNT(*)
#         FROM "Main"
#         WHERE "Ad-set Name" IS NOT NULL
#         GROUP BY "Ad-set Name"
#         ORDER BY COUNT(*) DESC
#         LIMIT 8
#     """)
#     top_adsets = cur.fetchall()

#         # 🔥 Insight 1: Top Course This Month
#     cur.execute("""
#         SELECT "Course", COUNT(*)
#         FROM "Main"
#         WHERE "Course" IS NOT NULL
#           AND "Date" ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
#           AND DATE_TRUNC('month', TO_DATE("Date",'DD/MM/YYYY')) =
#               DATE_TRUNC('month', CURRENT_DATE)
#         GROUP BY "Course"
#         ORDER BY COUNT(*) DESC
#         LIMIT 1
#     """)
#     top_course_month = cur.fetchone()


#     # 📣 Insight 2: Top Source vs Second Source
#     cur.execute("""
#         SELECT "Source", COUNT(*)
#         FROM "Main"
#         WHERE "Source" IS NOT NULL
#         GROUP BY "Source"
#         ORDER BY COUNT(*) DESC
#         LIMIT 2
#     """)
#     top_sources_compare = cur.fetchall()


#     # 🌍 Insight 3: Country dominance for top course
#     top_course_name = top_course_month[0] if top_course_month else None

#     country_for_course = None
#     if top_course_name:
#         cur.execute("""
#             SELECT "Target Country", COUNT(*)
#             FROM "Main"
#             WHERE "Course" = %s
#               AND "Target Country" IS NOT NULL
#             GROUP BY "Target Country"
#             ORDER BY COUNT(*) DESC
#             LIMIT 1
#         """, (top_course_name,))
#         country_for_course = cur.fetchone()

#     cur.close()
#     conn.close()

#     return {
#         "leads_by_month": leads_by_month,
#         "top_courses": top_courses,
#         "top_courses_month": top_courses_month,
#         "sources": sources,
#         "top_campaigns": top_campaigns,
#         "top_adsets": top_adsets,

#         # 🔥 Insights
#         "insights": {
#             "top_course_month": top_course_month,
#             "top_sources_compare": top_sources_compare,
#             "country_for_course": country_for_course
#         }
#     }
    
# @app.route("/api/distinct/<column>")
# def api_distinct(column):
#     conn = get_connection()
#     cur = conn.cursor()

#     cur.execute(f'''
#         SELECT DISTINCT "{column}"
#         FROM "Main"
#         WHERE "{column}" IS NOT NULL
#         AND TRIM("{column}"::text) <> ''
#         ORDER BY "{column}"::text
#         LIMIT 500
#     ''')

#     values = [r[0] for r in cur.fetchall()]

#     cur.close()
#     conn.close()

#     return jsonify(values)


# @app.route("/")
# def index():
#     if "user_id" not in session:
#         return redirect("/login")
#     page = int(request.args.get("page", 1))
#     limit = int(request.args.get("limit", 100))
#     offset = (page - 1) * limit

#     search = request.args.get("search")

#     where_clauses = []
#     params = []

#     # 🔍 Global search
#     if search:
#         where_clauses.append("""
#             EXISTS (
#                 SELECT 1
#                 FROM jsonb_each_text(to_jsonb("Main"))
#                 WHERE value ILIKE %s
#             )
#         """)
#         params.append(f"%{search}%")

#     # 🔥 MULTI-SELECT COLUMN FILTERS
#     selected_filters = {}
#     for key in request.args:
#         if key.startswith("filter_"):
#             column = key.replace("filter_", "")
#             values = request.args.getlist(key)
#             if values:
#                 selected_filters[column] = values
#                 sub = []
#                 for v in values:
#                     sub.append(f"\"{column}\" ILIKE %s")
#                     params.append(f"%{v}%")
#                 where_clauses.append("(" + " OR ".join(sub) + ")")

#     where_sql = ""
#     if where_clauses:
#         where_sql = "WHERE " + " AND ".join(where_clauses)

#     query_data = f"""
#     SELECT
#     ROW_NUMBER() OVER (ORDER BY "created_time" DESC) AS "rownum",
# ctid AS "__ctid",
#         "Date"                         AS "DATE",
#         "Time"                         AS "TIME",
#         "Full Name"                    AS "FULL NAME",
#         "Phone Number"                 AS "PHONE NUMBER",
#         "Email"                        AS "EMAIL",
#         "City"                         AS "CITY",
#         "State"                        AS "STATE",
#         "Course_City"                  AS "COURSE_CITY",
#         "Course_State"                 AS "COURSE_STATE",
#         "Course"                       AS "COURSE",
#         "Target Country"               AS "TARGET COUNTRY",
#         "Intake Year"                  AS "INTAKE YEAR",
#         "Target College Name"          AS "TARGET COLLEGE NAME",
#         "Target College State"         AS "TARGET COLLEGE STATE",
#         "Source"                       AS "SOURCE",
#         "created_time"                 AS "CREATED_TIME",
#         "Ad-set Name"                  AS "AD-SET NAME",
#         "Ad-set ID"                    AS "AD-SET ID",
#         "Form Name"                    AS "FORM NAME",
#         "Campaign Name"                AS "CAMPAIGN NAME",
#         "Number_Course"                AS "NUMBER_COURSE",
#         "Mode"                         AS "MODE",
#         "Form Id"                      AS "FORM ID",
#         "Database Creation Date"       AS "DATABASE CREATION DATE",
#         "Database Creation Time"       AS "DATABASE CREATION TIME",
#         "Number_Course 2"              AS "NUMBER_COURSE 2",
#         "Spreadsheet Source"           AS "SPREADSHEET SOURCE"
#     FROM "Main"
#     {where_sql}
#     ORDER BY "created_time" DESC
#     LIMIT %s OFFSET %s
# """

#     query_count = f"""
#         SELECT COUNT(*)
#         FROM "Main"
#         {where_sql}
#     """

#     conn = get_connection()
#     cur = conn.cursor()

#     # Fetch table data
#     cur.execute(query_data, params + [limit, offset])
#     rows = cur.fetchall()
#     headers = [desc[0] for desc in cur.description]

#     # Count
#     cur.execute(query_count, params)
#     total = cur.fetchone()[0]


#     # 🔁 Build WHERE clause for cascading filters
#     filter_where_clauses = []
#     filter_params = []

#     for col, values in selected_filters.items():
#         sub = []
#         for v in values:
#             sub.append(f"\"{col}\"::text ILIKE %s")
#             filter_params.append(f"%{v}%")
#         filter_where_clauses.append("(" + " OR ".join(sub) + ")")

#     base_filter_where_sql = ""
#     if filter_where_clauses:
#         base_filter_where_sql = "WHERE " + " AND ".join(filter_where_clauses)
#     # 🔥 FETCH DISTINCT FILTER VALUES
# # 🔥 FETCH DISTINCT FILTER VALUES (TYPE SAFE)
# # 🔥 FETCH ALL COLUMNS DYNAMICALLY
#     columns = get_table_columns(cur, "Main")

#     filter_options = {}

#     for col, data_type in columns:

#         if data_type in ("json", "jsonb", "bytea"):
#             continue

#         # ❗ exclude current column from applied filters
#         other_filters = []
#         other_params = []

#         for f_col, f_vals in selected_filters.items():
#             if f_col == col:
#                 continue
#             sub = []
#             for v in f_vals:
#                 sub.append(f"\"{f_col}\"::text ILIKE %s")
#                 other_params.append(f"%{v}%")
#             other_filters.append("(" + " OR ".join(sub) + ")")

#         other_where_sql = ""
#         if other_filters:
#             other_where_sql = "WHERE " + " AND ".join(other_filters)

#         # TEXT columns
#         if data_type in ("character varying", "text", "character"):
#             cur.execute(f"""
#                 SELECT DISTINCT "{col}"
#                 FROM "Main"
#                 {other_where_sql}
#                 {"AND" if other_where_sql else "WHERE"} "{col}" IS NOT NULL
#                 AND TRIM("{col}"::text) <> ''
#                 ORDER BY "{col}"::text
#                 LIMIT 500
#             """, other_params)
#             filter_options[col] = [r[0] for r in cur.fetchall()]

#         # NUMERIC
#         elif data_type in ("integer", "bigint", "numeric", "smallint"):
#             cur.execute(f"""
#                 SELECT DISTINCT "{col}"
#                 FROM "Main"
#                 {other_where_sql}
#                 {"AND" if other_where_sql else "WHERE"} "{col}" IS NOT NULL
#                 ORDER BY "{col}"
#                 LIMIT 500
#             """, other_params)
#             filter_options[col] = [str(r[0]) for r in cur.fetchall()]

#         # DATE / TIMESTAMP
#         elif data_type.startswith("timestamp") or data_type == "date":
#             cur.execute(f"""
#                 SELECT DISTINCT "{col}"::text
#                 FROM "Main"
#                 {other_where_sql}
#                 {"AND" if other_where_sql else "WHERE"} "{col}" IS NOT NULL
#                 ORDER BY "{col}" DESC
#                 LIMIT 200
#             """, other_params)
#             filter_options[col] = [r[0] for r in cur.fetchall()]

#     orig_columns = [col for col, dtype in get_table_columns(cur, "Main")]      

#     cur.close()
#     conn.close()

#     data = [dict(zip(headers, row)) for row in rows]
    

#     return render_template(
#         "index.html",
#         headers=headers,
#         table_columns=orig_columns,
#         data=data,
#         page=page,
#         total=total,
#         limit=limit,
#         search=search or "",
#         filter_options=filter_options,
#         selected_filters=selected_filters
#     )


# # -------------------------------
# # AUTO TRIGGER SYSTEM (IMPORTANT)
# # -------------------------------

# scheduler = BackgroundScheduler()
# scheduler.start()


# def auto_send_job(setting_id):
#     conn = get_connection()
#     cur = conn.cursor()

#     # Update last trigger timestamp
#     cur.execute("""
#         UPDATE saved_settings
#         SET last_trigger_run = NOW()
#         WHERE id = %s
#         RETURNING mappings, sheet_url, automation_rules, switch_rules,
#           daily_ro, daily_sent, last_sent_date
#     """, (setting_id,))
    
#     row = cur.fetchone()
#     conn.commit()

#     if not row:
#         print("⚠️ No automation found for ID", setting_id)
#         return

#     mappings, sheet_url, rules, switch_rules, daily_ro, daily_sent, last_sent_date = row

#     # ============================================
#     # 1️⃣ RESET DAILY COUNTER AT MIDNIGHT
#     # ============================================
#     today = date.today()
#     if last_sent_date != today:
#         daily_sent = 0
#         cur.execute("""
#             UPDATE saved_settings
#             SET daily_sent = 0,
#                 last_sent_date = %s
#             WHERE id = %s
#         """, (today, setting_id))
#         conn.commit()

#     # ============================================
#     # 2️⃣ CHECK DAILY LIMIT
#     # ============================================
#     if daily_ro is not None and daily_sent >= daily_ro:
#         print(f"⛔ DAILY LIMIT REACHED for campaign {setting_id}")
#         cur.close()
#         conn.close()
#         return

#     remaining = (daily_ro - daily_sent) if daily_ro else None

#     # Convert JSON strings → objects
#     if isinstance(mappings, str):
#         mappings = json.loads(mappings)

#     if isinstance(rules, str):
#         rules = json.loads(rules)

#     # --------------------------------------------
#     # Build SQL based on automation rules
#     # --------------------------------------------
#     extra_conditions = []
#     params = []

#     for rule in rules:
#         col = rule["field"]
#         values = rule["value"]

#         placeholders = ", ".join(["%s"] * len(values))
#         extra_conditions.append(f'"{col}" IN ({placeholders})')
#         params.extend(values)

#     where_sql = " AND ".join(extra_conditions) if extra_conditions else "TRUE"

#     sql = f"""
#         SELECT *
#         FROM "Main"
#         WHERE {where_sql}
#         ORDER BY created_time DESC
#         LIMIT %s
#     """

#     # Limit by remaining quota
#     fetch_limit = remaining if remaining else 1000
#     params.append(fetch_limit)

#     cur.execute(sql, params)
#     rows = cur.fetchall()
#     headers = [desc[0] for desc in cur.description]

#     if not rows:
#         print("ℹ No matching leads found")
#         cur.close()
#         conn.close()
#         return

#     # ----------------------------------------
#     # Map rows
#     # ----------------------------------------
#     mapped_rows = []
#     for r in rows:
#         row_dict = dict(zip(headers, r))
#         mapped = {}

#         for m in mappings:
#             table_key = m["tableField"]
#             sheet_key = m["sheetField"]
#             mapped[sheet_key] = row_dict.get(table_key)

#         mapped_rows.append(mapped)

#     # ----------------------------------------
#     # Send to Google Sheet
#     # ----------------------------------------
#     try:
#         r = requests.post(
#             GOOGLE_SCRIPT_URL,
#             json={
#                 "sheetUrl": sheet_url,
#                 "rows": mapped_rows,
#                 "mappings": mappings
#             },
#             timeout=60
#         )

#         gs_response = r.json()

#         return jsonify({
#             "success": True,
#             "sent": len(mapped_rows),
#             "gs_response": gs_response
#         })

#     except Exception as e:
#         return jsonify({
#             "success": False,
#             "error": str(e)
#         }), 500

#         # ----------------------------------------
#         # 3️⃣ Update daily_sent count
#         # ----------------------------------------
#         sent_now = len(mapped_rows)
#         cur.execute("""
#             UPDATE saved_settings
#             SET daily_sent = daily_sent + %s,
#                 last_sent_date = %s
#             WHERE id = %s
#         """, (sent_now, today, setting_id))
#         conn.commit()

#         print(f"✅ Sent {sent_now} leads (Total Today: {daily_sent + sent_now})")

#     except Exception as e:
#         print("❌ Auto Trigger Error:", e)

#     cur.close()
#     conn.close()



# if __name__ == "__main__":
#     app.run(port=5000, debug=True)
