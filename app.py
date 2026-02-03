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
SEND_STATE = {"stop": False, "cancel": False, "index": 0}
from apscheduler.schedulers.background import BackgroundScheduler
import json
from datetime import date
import time

# 🔥 SIMPLE IN-MEMORY CACHE
FILTER_CACHE = {}
CACHE_TTL = 300 # 5 minutes


DB_USER = os.getenv("SUPABASE_DB_USER")
DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
DB_HOST = os.getenv("SUPABASE_DB_HOST")
DB_PORT = os.getenv("SUPABASE_DB_PORT", "5432")
DB_NAME = os.getenv("SUPABASE_DB_NAME")
GOOGLE_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbzeorj-ZaDZdxcbfCVw36PpxnblVQfrfXc4eQphfDVGBn9T_0vXK_tEkYjaDSvGqSLm/exec"

from datetime import datetime, timedelta

def fix_date(d):
    if not d:
        return ""

    # If Google Sheet gives ISO UTC timestamp
    if isinstance(d, str) and "T" in d:
        try:
            # Parse the UTC datetime without timezone
            dt = datetime.strptime(d.replace("Z",""), "%Y-%m-%dT%H:%M:%S.%f")
        except:
            dt = datetime.strptime(d.replace("Z",""), "%Y-%m-%dT%H:%M:%S")

        # Convert UTC → IST (+5:30)
        dt = dt + timedelta(hours=5, minutes=30)

        # Return YYYY-MM-DD
        return dt.strftime("%Y-%m-%d")

    # If DD/MM/YYYY from sheet
    if "/" in d:
        dd, mm, yyyy = d.split("/")
        return f"{yyyy}-{mm}-{dd}"

    return d
def get_connection():
    missing = []
    if not DB_HOST: missing.append("SUPABASE_DB_HOST")
    if not DB_USER: missing.append("SUPABASE_DB_USER")
    if not DB_PASSWORD: missing.append("SUPABASE_DB_PASSWORD")
    if not DB_NAME: missing.append("SUPABASE_DB_NAME")

    if missing:
        raise Exception(f"Missing env vars: {', '.join(missing)}")

    return psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        port=int(DB_PORT),
        connect_timeout=5
    )
def get_table_columns(cur, table_name):
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
    """, (table_name,))
    return cur.fetchall()


@app.route("/api/load-control-sheet", methods=["POST"])
def load_control_sheet():
    print("\n\n==================== 🔥 CONTROL SHEET DEBUG START ====================\n")

    data = request.json
    sheet_url = data.get("url")
    print("📥 Incoming URL:", sheet_url)

    if not sheet_url:
        print("❌ ERROR: No URL received")
        return jsonify({"error": "Missing URL"}), 400

    print("\n➡ Calling Google Script to read CONTROL SHEET...")

    try:
        r = requests.post(GOOGLE_SCRIPT_URL, data={
            "mode": "readControlSheet",
            "sheetUrl": sheet_url
        }, timeout=20)

        control = r.json()
        print("\n📄 CONTROL SHEET DATA LOADED (First 2 rows):")
        print(json.dumps(control[:2], indent=2))

    except Exception as e:
        print("❌ ERROR READING CONTROL SHEET:", str(e))
        return jsonify({"error": f"Error reading sheet: {e}"}), 500

    # -------------------------------------------------------
    # FIND ALL ACTIVE ROWS
    # -------------------------------------------------------
    active_rows = []
    for row in control:
        if str(row.get("Status", "")).strip().lower() == "active":
            active_rows.append(row)
    
    print(f"✔ FOUND {len(active_rows)} ACTIVE CAMPAIGNS")

    if not active_rows:
        return jsonify({"error": "No ACTIVE rows found"}), 404

    # -------------------------------------------------------
    # LOAD ALL SHEETS ONCE (OPTIMIZATION)
    # -------------------------------------------------------
    def fetch_sheet(name):
        try:
            r = requests.post(GOOGLE_SCRIPT_URL, data={
                "mode": "readSpecificSheet",
                "sheetUrl": sheet_url,
                "sheetName": name
            })
            return r.json()
        except:
            return []

    print("➡ Fetching auxiliary sheets...")
    rules_sheet_all = fetch_sheet("Manual Automation Rules")
    cm_sheet_all = fetch_sheet("Conditional Multi-Mapping")
    field_map_sheet_all = fetch_sheet("Field Mapping")
    print("✔ Auxiliary sheets loaded.")


    # -------------------------------------------------------
    # BUILD CONFIG FOR EACH CAMPAIGN
    # -------------------------------------------------------
    campaigns = []

    for active in active_rows:
        campaign_id = active.get("Campaign ID")
        print(f"\n⚙ Processing Campaign ID: {campaign_id}")

        # 1. MATCH RULES
        matched_rules = [
            r for r in rules_sheet_all 
            if str(r.get("Campaign ID")).strip() == str(campaign_id).strip()
            and str(r.get("Status")).strip().lower() == "active"
        ]

        auto_rules = []
        for r in matched_rules:
            db_field = str(r.get("DB Field", "")).strip()
            # Handle multiple value keys
            value = (r.get("value") or r.get("Value") or r.get("values") or r.get("Values") or "")
            values = [v.strip() for v in str(value).split(",") if v.strip()]
            
            auto_rules.append({
                "field": db_field.strip(),
                "values": values
            })

        # 🔥 AUTO-INJECT CAMPAIGN ID RULE if not present
        # This prevents "Runaway Campaign" (sending data from other campaigns)
        has_id_filter = any(r["field"].lower() in ["campaign id", "campaign name"] for r in auto_rules)
        if not has_id_filter and campaign_id:
             print(f"   ⚠ No User Rule for Campaign ID. Auto-injecting filter: Campaign ID = {campaign_id}")
             auto_rules.append({
                 "field": "Campaign ID",
                 "values": [str(campaign_id).strip()]
             })

        # 2. MATCH CONDITIONAL MULTI-MAPPING
        cm_rules = [
            r for r in cm_sheet_all
            if str(r.get("Campaign ID")).strip() == str(campaign_id).strip()
            and str(r.get("Status", "")).strip().lower() == "active"
        ]
        
        # Group CM rules
        grouped = {}
        for r in cm_rules:
            key = (r.get("DB Field"), r.get("Match"))
            if key not in grouped: grouped[key] = []
            grouped[key].append(r)

        conditional_multi = []
        for (dbf, match), rows2 in grouped.items():
            obj = {"field": dbf, "match": match, "outputs": []}
            for x in rows2:
                obj["outputs"].append({
                    "header": x.get("Output Header"),
                    "value": x.get("Output Value")
                })
            conditional_multi.append(obj)

        # 3. MATCH FIELD MAPPING
        field_mapping = [
            r for r in field_map_sheet_all
            if str(r.get("Campaign ID")).strip() == str(campaign_id).strip()
            and str(r.get("Status", "")).strip().lower() == "active"
        ]

        # ⚠ WARNING FOR SPARSE MAPPINGS
        if len(field_mapping) < 2:
            print(f"   ❌ WARNING: Campaign {campaign_id} has very few mappings ({len(field_mapping)}). Check 'Field Mapping' sheet (rows for Campaign ID {campaign_id})!")

        # BUILD CAMPAIGN OBJECT
        s_name = str(active.get("Sheet Name", "") or active.get("Tab Name", "")).strip()
        print(f"   -> Extracted Sheet Name: '{s_name}'")
        print(f"   -> Field Mappings Count: {len(field_mapping)}")

        camp_obj = {
            "campaign_id": campaign_id,
            "sheet_url": active.get("Google Sheet URL", ""),
            "sheet_name": s_name,
            "daily_ro": active.get("Daily RO (limit)", ""),
            "start_date": fix_date(active.get("Start Date", "")),
            "end_date": fix_date(active.get("End Date", "")),
            "auto_rules": auto_rules,
            "conditional_multi": conditional_multi,
            "field_mapping": field_mapping
        }
        campaigns.append(camp_obj)

    print("\n==================== ✔ RETURNING MULTI-CAMPAIGN RESPONSE ====================\n")
    
    # Return list of campaigns
    # We wrap it in a "campaigns" key, but also return the first one as root keys for backward compat if needed (though we plan to change frontend)
    resp = {
        "campaigns": campaigns,
        "count": len(campaigns)
    }
    
    return jsonify(resp)



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
@app.route("/save-settings", methods=["POST"])
def save_settings():
    data = request.json

    name = data.get("name")
    mappings = data.get("mappings", [])
    sheet_url = data.get("sheetUrl")
    rules = data.get("automationRules", [])
    switch_rules = data.get("switchRules", [])
    trigger_interval = data.get("triggerInterval")
    daily_ro = data.get("dailyRo")

    # Convert daily RO
    if daily_ro in ("", None, "null"):
        daily_ro = None
    else:
        daily_ro = int(daily_ro)

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO saved_settings
        (user_id, name, sheet_url, mappings, automation_rules, switch_rules,
         trigger_interval, daily_ro, created_by, created_at, sheet_name)
        VALUES (%s, %s, %s, %s, %s, %s,
                %s, %s, %s, NOW(), %s)
        RETURNING id
    """, (
        session["user_id"],
        name,
        sheet_url,
        json.dumps(mappings),
        json.dumps(rules),
        json.dumps(switch_rules),
        trigger_interval,
        daily_ro,
        session["user_email"],
        data.get("sheetName")
    ))

    new_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()

    return jsonify({"message": "Saved successfully", "id": new_id})

@app.route("/saved-campaigns")
def saved_campaigns():
    if "user_id" not in session:
        return redirect("/login")

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
       SELECT id, name, query_string, sheet_url, trigger_interval, created_at, created_by
        FROM saved_settings
        WHERE user_id = %s
        ORDER BY id DESC
    """, (session["user_id"],))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return render_template("saved_campaigns.html", campaigns=rows)



@app.route("/saved-campaigns/<int:id>")
def saved_campaign_detail(id):
    if "user_id" not in session:
        return redirect("/login")

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
SELECT id, name, query_string, mappings,
       sheet_url, trigger_interval,
       automation_rules, daily_ro,
       updated_by, updated_at, last_trigger_run,
       created_by, created_at
FROM saved_settings
WHERE id = %s AND user_id = %s
    """, (id, session["user_id"]))

    row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        return "Campaign not found", 404

    return render_template("saved_campaign_detail.html", campaign=row)




@app.route("/saved-campaigns/delete/<int:id>", methods=["POST"])
def delete_campaign(id):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        DELETE FROM saved_settings
        WHERE id = %s AND user_id = %s
    """, (id, session["user_id"]))

    conn.commit()
    cur.close()
    conn.close()

    # 🔥 Delete trigger if running
    job_id = f"job_{id}"
    try:
        scheduler.remove_job(job_id)
        print(f"🛑 Deleted trigger {job_id} because campaign was deleted by user")
    except Exception:
        pass

    return redirect("/saved-campaigns")


@app.route("/saved-campaigns/edit/<int:id>", methods=["GET", "POST"])
def edit_campaign(id):
    if request.method == "POST":
        name = request.form.get("name")
        sheet_url = request.form.get("sheet_url")
        sheet_name = request.form.get("sheet_name")
        interval = request.form.get("interval")

        mappings = json.loads(request.form.get("mappings_json") or "[]")
        rules = json.loads(request.form.get("rules_json") or "[]")
        switch_rules = json.loads(request.form.get("switch_json") or "[]")

        daily_ro = request.form.get("daily_ro")
        if daily_ro in ("", None, "null"):
            daily_ro = None
        else:
            daily_ro = int(daily_ro)

        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
        UPDATE saved_settings
        SET name = %s,
            sheet_url = %s,
            trigger_interval = %s,
            mappings = %s,
            automation_rules = %s,
            daily_ro = %s,
            updated_by = %s,
            updated_at = NOW(),
            switch_rules = %s,
            sheet_name = %s
        WHERE id = %s AND user_id = %s
    """, (
        name,
        sheet_url,
        interval,
        json.dumps(mappings),
        json.dumps(rules),
        daily_ro,                    # ✔ correct
        session["user_email"],
        json.dumps(switch_rules),    # ✔ correct
        sheet_name,
        id,
        session["user_id"]
    ))

        conn.commit()
        cur.close()
        conn.close()

        return redirect(f"/saved-campaigns/{id}")

    # ---------------------------
    # GET — Load existing campaign
    # ---------------------------

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, name, query_string, mappings,
       sheet_url, trigger_interval,
       automation_rules, daily_ro,
       updated_by, updated_at, last_trigger_run,
       created_by, created_at, switch_rules, sheet_name
FROM saved_settings
        WHERE id = %s AND user_id = %s
    """, (id, session["user_id"]))

    row = cur.fetchone()

    # Load table columns
    columns = get_table_columns(cur, "Main")
    table_columns = [col for col, dtype in columns]

    cur.close()
    conn.close()

    if not row:
        return "Campaign not found", 404

    # ---------------------------
    # SAFE JSONB LOADING FIX
    # ---------------------------

    raw_mappings = row[3]
    raw_rules = row[6]

    mappings = raw_mappings if isinstance(raw_mappings, list) else json.loads(raw_mappings or "[]")
    rules = raw_rules if isinstance(raw_rules, list) else json.loads(raw_rules or "[]")
    raw_switch = row[13]

    switch_rules = raw_switch if isinstance(raw_switch, list) else json.loads(raw_switch or "[]")

    return render_template(
        "edit_campaign_full.html",
        campaign=row,
        mappings_json=mappings,
        rules_json=rules,
        switch_json=switch_rules,
        table_columns=table_columns
    )





@app.route("/start-trigger", methods=["POST"])
def start_trigger():
    data = request.json
    setting_id = data["id"]
    interval = int(data["interval"])

    scheduler.add_job(
        func=auto_send_job,
        trigger='interval',
        minutes=interval,
        args=[setting_id],
        id=f"job_{setting_id}",
        replace_existing=True
    )

    return jsonify({"message": f"Trigger set every {interval} minutes"})




@app.route("/send-to-sheet", methods=["POST"])
def send_to_sheet():
    print("\n\n================== 🔥 DEBUG START (MANUAL SEND) ==================\n")

    payload = request.json
    db_headers = payload.get("dbHeaders", [])
    orig_headers = db_headers[:]   # hard copy
    print("📥 RAW PAYLOAD:", json.dumps(payload, indent=2))

    sheet_url  = payload.get("sheetUrl")
    sheet_name = payload.get("sheetName")
    mappings   = payload.get("mappings", [])
    
    print(f"📌 PAYLOAD SHEET NAME: '{sheet_name}'")
    print(f"📌 PAYLOAD MAPPINGS ({len(mappings)}):")
    # print(json.dumps(mappings, indent=2)) 
    rules      = payload.get("automationRules", [])
    switch     = payload.get("switchRules", [])
    daily_ro   = payload.get("dailyRo")
    ctids      = payload.get("ctids", [])
    multiMaps = payload.get("multiMaps", [])
    print("\n📌 MULTI MAPS:")
    print(json.dumps(multiMaps, indent=2))

    print("\n📌 Sheet URL:", sheet_url)
    print("📌 Daily RO:", daily_ro)
    print("📌 Selected CTIDs:", ctids)
    print("\n📌 MAPPINGS:")
    print(json.dumps(mappings, indent=2))

    print("\n📌 MANUAL RULES:")

    if ctids and not rules:
        print("📌 Selected rows ONLY (no auto fetch because no rules applied)")
        auto_rows = []

    if daily_ro in ("", None, "null"): 
        daily_ro = None
    else:
        daily_ro = int(daily_ro)

    conn = get_connection()
    cur = conn.cursor()
    # ✅ SAFETY: headers must exist even if no auto rows
    if 'headers' not in locals():
        headers = [col for col in payload.get("dbHeaders", [])]


    # -----------------------------------------------------------------
    # 1️⃣ BUILD SQL FROM RULES
    # -----------------------------------------------------------------
    conditions = []
    params = []
    start_date = payload.get("startDate")
    end_date   = payload.get("endDate")

    if start_date and end_date:
        conditions.append("""
            TO_DATE("Date",'DD/MM/YYYY')
            BETWEEN %s AND %s
        """)
        params.extend([start_date, end_date])
        print("📅 DATE RANGE APPLIED:", start_date, "→", end_date)

    for r in rules:
        print("\n🔍 RULE PROCESSING:", r)

        print("\n================= 📘 APPLIED FILTERS =================")
        if not rules:
            print("📘 (No filters applied)")
        else:
            for rr in rules:
                vals = rr.get("value") or rr.get("values") or []
                print(f"📘 {rr['field']} IN {vals}")
        print("=====================================================\n")
        

        col = r["field"]
        values = r.get("value") or r.get("values") or []

        placeholders = ", ".join(["%s"] * len(values))
        sql_part = f'"{col}"::text IN ({placeholders})'
        conditions.append(sql_part)
        params.extend(values)

        print("   ➤ SQL PART:", sql_part)
        print("   ➤ PARAMS:", values)

    where_sql = " AND ".join(conditions) if conditions else "TRUE"
    print("\n📝 FINAL WHERE SQL:", where_sql)
    print("📝 FINAL PARAM LIST:", params)

    fetch_limit = daily_ro if daily_ro else 5000
    print("\n📌 Fetch limit:", fetch_limit)

    # -----------------------------------------------------------------
    # 2️⃣ FETCH AUTO RULE ROWS
    # -----------------------------------------------------------------
    sql = f"""
        SELECT *
        FROM "Main"
        WHERE {where_sql}
        ORDER BY created_time DESC
        LIMIT %s
    """

    print("\n⚡ FINAL SQL EXECUTED:")
    print(sql)

    try:
        cur.execute(sql, params + [fetch_limit])
        auto_rows = cur.fetchall()

        if cur.description:
            headers = [d[0] for d in cur.description]
        else:
            headers = orig_headers  # Safe fallback headers from frontend # <— BEST FIX


        print("\n📥 AUTO ROWS FETCHED:", len(auto_rows))
        if len(auto_rows) > 0:
            print("📌 First Row Sample:", dict(zip(headers, auto_rows[0])))
    except Exception as e:
        print("\n❌ SQL ERROR:", str(e))
        return jsonify({"error": "SQL ERROR", "details": str(e)}), 500

    # -----------------------------------------------------------------
    # 3️⃣ FETCH SELECTED ROWS
    # -----------------------------------------------------------------
    selected_rows = []
    if ctids:
        try:
            cur.execute("""
                SELECT *
                FROM "Main"
                WHERE ctid = ANY(%s::tid[])
            """, (ctids,))
            selected_rows = cur.fetchall()
            print("\n📥 SELECTED ROWS FETCHED:", len(selected_rows))
        except Exception as e:
            print("\n❌ CTID FETCH ERROR:", e)

    cur.close()
    conn.close()

    # -----------------------------------------------------------------
    # 4️⃣ MERGE + DEDUPE
    # -----------------------------------------------------------------
    all_rows = auto_rows + selected_rows
    print("\n📊 TOTAL ROWS BEFORE DEDUPE:", len(all_rows))

    seen = set()
    unique = []

    for r in all_rows:
        row_dict = dict(zip(headers, r))
        unique_key = row_dict.get("Number_Course 2") or row_dict.get("Phone Number")

        if unique_key not in seen:
            seen.add(unique_key)
            unique.append(r)

    print("📊 TOTAL ROWS AFTER DEDUPE:", len(unique))

    final_rows = unique

    if daily_ro:
        final_rows = final_rows[:daily_ro]

    print("📌 FINAL ROW COUNT AFTER DAILY RO:", len(final_rows))

    # -----------------------------------------------------------------
    # 5️⃣ APPLY MAPPING + MULTI-MAPPING  (FIXED)
    # -----------------------------------------------------------------
    # -----------------------------------------------------------------
# 5️⃣ CLEAN + STABLE MULTI-MAPPING ENGINE (FINAL)
# -----------------------------------------------------------------
    mapped_rows = []

    print("\n================ MULTI-MAPPING ENGINE START ================\n")
    print("📌 MultiMaps received from frontend:")
    print(json.dumps(multiMaps, indent=2))
    print("\n============================================================\n")

    for r in final_rows:

        if not headers:
            print("❌ HEADERS EMPTY — SKIPPING ROW")
            continue

        row_dict = dict(zip(headers, r))

        print("\n------------------------------------------------------------")
        print("Processing NEW ROW:")
        print(json.dumps(row_dict, indent=2))
        print("------------------------------------------------------------\n")

        mapped = {}

        # ============================================================
        # FIRST — APPLY MULTI-MAPPING RULES
        # ============================================================
        if not multiMaps:
            print("ℹ No Conditional Multi-Mapping rules provided\n")
        else:
            print("🔍 Checking Conditional Multi-Mapping rules...\n")

        for rule in multiMaps or []:

            print("🟦 MULTI-MAP RULE:")
            print(json.dumps(rule, indent=2))

            field   = rule.get("field")
            match   = rule.get("match")
            outputs = rule.get("outputs", [])

            print(f"→ Field to check: {field}")
            print(f"→ Match value   : {match}")

            if not field or not match:
                print("❌ Invalid rule (missing field or match) — SKIPPING")
                continue

            if field not in row_dict:
                print("❌ Field not found in DB row — SKIPPING RULE")
                continue

            raw_db = str(row_dict.get(field, "")).strip()
            clean_db = raw_db.lower().replace(".", "")
            clean_match = match.strip().lower().replace(".", "")
            clean_db_list = [x.strip().lower().replace(".", "")
                            for x in raw_db.split(",")]

            print("→ Raw DB Value   :", raw_db)
            print("→ Clean DB Value :", clean_db)
            print("→ Clean list     :", clean_db_list)
            print("→ Clean match    :", clean_match)

            hit = False

            if clean_db == clean_match:
                print("✔ EXACT MATCH")
                hit = True

            elif clean_match in clean_db_list:
                print("✔ LIST ITEM MATCH")
                hit = True

            elif clean_db.startswith(clean_match):
                print("✔ STARTSWITH MATCH")
                hit = True

            elif clean_match in clean_db:
                print("✔ SUBSTRING MATCH")
                hit = True

            else:
                print("❌ No match — Rule skipped")

            if hit:
                print("🎯 MATCH SUCCESS — applying outputs...")
                for o in outputs:
                    h = o.get("header")
                    v = o.get("value")
                    mapped[h] = v
                    print(f"   → SET {h} = {v}")

            print("🟥 RULE END\n")

        # ============================================================
        # SECOND — APPLY NORMAL MAPPINGS
        # ============================================================
        print("📌 Applying normal mappings...\n")

        for m in mappings:

            table_field = m.get("tableField")
            sheet_field = m.get("sheetField")
            static_val  = m.get("staticValue")

            print(f"➡ Mapping TABLE={table_field}, SHEET={sheet_field}, STATIC={static_val}")

            # Static value always wins
            if static_val not in ("", None):
                mapped[sheet_field] = static_val
                print("✔ Static applied:", static_val)
                continue

            # If already set by Multi-Map → skip
            if sheet_field in mapped:
                print("⏭ Skipped (already filled by Multi-Map)")
                continue

            # Normal DB mapping
            if table_field not in row_dict:
                print("❌ DB column missing — writing empty string")
                mapped[sheet_field] = ""
            else:
                mapped[sheet_field] = row_dict.get(table_field)
                print(f"✔ Mapped {table_field} → {sheet_field} = {row_dict.get(table_field)}")

        print("\nFINAL ROW MAPPED:")
        print(json.dumps(mapped, indent=2))
        print("------------------------------------------------------------\n")

        mapped_rows.append(mapped)

    print("\n📤 FINAL MAPPED ROWS TO SEND:", len(mapped_rows))
    print(json.dumps(mapped_rows[:3], indent=2))

    # -----------------------------------------------------------------
    # 6️⃣ CONTROLLED SENDING LOOP
    # -----------------------------------------------------------------
# -----------------------------------------------------------------
# 6️⃣ STREAMING SEND LOOP (FULL FIX)
# -----------------------------------------------------------------

    def generate_stream():
        global SEND_STATE

        SEND_STATE["stop"] = False
        SEND_STATE["cancel"] = False
        SEND_STATE["index"] = 0

        total_rows = len(mapped_rows)
        yield f"📤 Sending started... Total rows: {total_rows}\n"

        BATCH_SIZE = 50

        for start in range(0, len(mapped_rows), BATCH_SIZE):

            # Stop or cancel logic
            if SEND_STATE["stop"]:
                yield f"⏸️ Sending paused at batch starting {start+1}\n"
                return

            if SEND_STATE["cancel"]:
                yield "❌ Sending cancelled by user.\n"
                SEND_STATE["index"] = 0
                return

            batch = mapped_rows[start:start + BATCH_SIZE]

            yield f"➡ Sending rows {start+1} to {start+len(batch)}...\n"

            try:
                res = requests.post(
                    GOOGLE_SCRIPT_URL,
                    data={
                        "sheetUrl": sheet_url,
                        "rows": json.dumps(batch)   # ⭐ SENDING 50 ROWS AT ONCE
                    },
                    timeout=40
                )

                yield f"   ✔ Batch Response: {res.text[:150]}\n"

            except Exception as e:
                yield f"❌ SEND ERROR: {str(e)}\n"

        SEND_STATE["index"] = 0
        yield "✅ Completed all rows\n"

    return Response(generate_stream(), mimetype="text/plain")







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
    
@app.route("/api/distinct/<column>")
def api_distinct(column):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(f'''
        SELECT DISTINCT "{column}"
        FROM "Main"
        WHERE "{column}" IS NOT NULL
        AND TRIM("{column}"::text) <> ''
        ORDER BY "{column}"::text
        LIMIT 500
    ''')

    values = [r[0] for r in cur.fetchall()]

    cur.close()
    conn.close()

    return jsonify(values)


@app.route("/send-control", methods=["POST"])
def send_control():
    data = request.json
    action = data.get("action")

    if action == "stop":
        SEND_STATE["stop"] = True
        return {"status": "stopped"}

    if action == "cancel":
        SEND_STATE["cancel"] = True
        SEND_STATE["index"] = 0
        return {"status": "cancelled"}

    if action == "resume":
        SEND_STATE["stop"] = False
        SEND_STATE["cancel"] = False
        return {"status": "resumed"}

    return {"status": "unknown"}

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
    ROW_NUMBER() OVER (ORDER BY to_date("Date", 'DD/MM/YYYY') DESC NULLS LAST, "created_time" DESC) AS "rownum",
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
    ORDER BY to_date("Date", 'DD/MM/YYYY') DESC NULLS LAST, "created_time" DESC
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
            # ⚡ CACHE KEY
            cache_key = f"{col}_{hash(tuple(other_params))}"
            now = time.time()

            # Check Cache
            if cache_key in FILTER_CACHE:
                entry = FILTER_CACHE[cache_key]
                if now - entry['time'] < CACHE_TTL:
                    filter_options[col] = entry['data']
                    continue # Skip DB query

            cur.execute(f"""
                SELECT DISTINCT "{col}"
                FROM "Main"
                {other_where_sql}
                {"AND" if other_where_sql else "WHERE"} "{col}" IS NOT NULL
                AND TRIM("{col}"::text) <> ''
                ORDER BY "{col}"::text
                LIMIT 500
            """, other_params)
            val = [r[0] for r in cur.fetchall()]
            filter_options[col] = val
            
            # Store in Cache
            FILTER_CACHE[cache_key] = {'data': val, 'time': now}

        # NUMERIC
        elif data_type in ("integer", "bigint", "numeric", "smallint"):
            cache_key = f"{col}_{hash(tuple(other_params))}_num"
            now = time.time()

            if cache_key in FILTER_CACHE:
                entry = FILTER_CACHE[cache_key]
                if now - entry['time'] < CACHE_TTL:
                    filter_options[col] = entry['data']
                    continue

            cur.execute(f"""
                SELECT DISTINCT "{col}"
                FROM "Main"
                {other_where_sql}
                {"AND" if other_where_sql else "WHERE"} "{col}" IS NOT NULL
                ORDER BY "{col}"
                LIMIT 500
            """, other_params)
            val = [str(r[0]) for r in cur.fetchall()]
            filter_options[col] = val
            FILTER_CACHE[cache_key] = {'data': val, 'time': now}

        # DATE / TIMESTAMP
        elif data_type.startswith("timestamp") or data_type == "date":
            # Dates change less often, but respecting filters is key
            cache_key = f"{col}_{hash(tuple(other_params))}_date"
            now = time.time()

            if cache_key in FILTER_CACHE:
                entry = FILTER_CACHE[cache_key]
                if now - entry['time'] < CACHE_TTL:
                    filter_options[col] = entry['data']
                    continue

            cur.execute(f"""
                SELECT DISTINCT "{col}"::text
                FROM "Main"
                {other_where_sql}
                {"AND" if other_where_sql else "WHERE"} "{col}" IS NOT NULL
                ORDER BY "{col}" DESC
                LIMIT 200
            """, other_params)
            val = [r[0] for r in cur.fetchall()]
            filter_options[col] = val
            FILTER_CACHE[cache_key] = {'data': val, 'time': now}

    orig_columns = [col for col, dtype in get_table_columns(cur, "Main")]      

    cur.close()
    conn.close()

    data = [dict(zip(headers, row)) for row in rows]
    

    return render_template(
        "index.html",
        headers=headers,
        table_columns=orig_columns,
        data=data,
        page=page,
        total=total,
        limit=limit,
        search=search or "",
        filter_options=filter_options,
        selected_filters=selected_filters
    )


# -------------------------------
# AUTO TRIGGER SYSTEM (IMPORTANT)
# -------------------------------

scheduler = BackgroundScheduler()
scheduler.start()


def auto_send_job(setting_id):
    conn = get_connection()
    cur = conn.cursor()

    # Update last trigger timestamp
    cur.execute("""
        UPDATE saved_settings
        SET last_trigger_run = NOW()
        WHERE id = %s
        RETURNING mappings, sheet_url, automation_rules, switch_rules,
          daily_ro, daily_sent, last_sent_date, sheet_name
    """, (setting_id,))
    
    row = cur.fetchone()
    conn.commit()

    if not row:
        print("⚠️ No automation found for ID", setting_id)
        return

    mappings, sheet_url, rules, switch_rules, daily_ro, daily_sent, last_sent_date, sheet_name = row

    # ============================================
    # 1️⃣ RESET DAILY COUNTER AT MIDNIGHT
    # ============================================
    today = date.today()
    if last_sent_date != today:
        daily_sent = 0
        cur.execute("""
            UPDATE saved_settings
            SET daily_sent = 0,
                last_sent_date = %s
            WHERE id = %s
        """, (today, setting_id))
        conn.commit()

    # ============================================
    # 2️⃣ CHECK DAILY LIMIT
    # ============================================
    if daily_ro is not None and daily_sent >= daily_ro:
        print(f"⛔ DAILY LIMIT REACHED for campaign {setting_id}")
        cur.close()
        conn.close()
        return

    remaining = (daily_ro - daily_sent) if daily_ro else None

    # Convert JSON strings → objects
    if isinstance(mappings, str):
        mappings = json.loads(mappings)

    if isinstance(rules, str):
        rules = json.loads(rules)

    # --------------------------------------------
    # Build SQL based on automation rules
    # --------------------------------------------
    extra_conditions = []
    params = []
    

    for rule in rules:
        col = rule["field"]
        values = rule["value"]

        placeholders = ", ".join(["%s"] * len(values))
        extra_conditions.append(f'"{col}" IN ({placeholders})')
        params.extend(values)

    where_sql = " AND ".join(extra_conditions) if extra_conditions else "TRUE"

    sql = f"""
        SELECT *
        FROM "Main"
        WHERE {where_sql}
        ORDER BY created_time DESC
        LIMIT %s
    """

    # Limit by remaining quota
    fetch_limit = remaining if remaining else 1000
    params.append(fetch_limit)

    cur.execute(sql, params)
    rows = cur.fetchall()
    headers = [desc[0] for desc in cur.description]

    if not rows:
        print("ℹ No matching leads found")
        cur.close()
        conn.close()
        return

    # ----------------------------------------
    # Map rows
    # ----------------------------------------
    mapped_rows = []
    for r in rows:
        row_dict = dict(zip(headers, r))
        mapped = {}

        for m in mappings:
            table_key = m["tableField"]
            sheet_key = m["sheetField"]
            mapped[sheet_key] = row_dict.get(table_key)

        mapped_rows.append(mapped)

    # Extract GID if possible for redundancy
    gid = None
    if "gid=" in sheet_url:
        try:
            gid = sheet_url.split("gid=")[1].split("#")[0]
        except:
            pass

    # ----------------------------------------
    # Send to Google Sheet
    # ----------------------------------------
    try:
        final_payload = {
            "sheetUrl": sheet_url,
            "sheetName": sheet_name,
            "sheet_name": sheet_name, # redundancy
            "tabName": sheet_name,    # redundancy
            "gid": gid,               # redundancy
            "rows": mapped_rows,
            "mappings": mappings
        }
        print(f"📤 Sending to Script. Sheet: '{sheet_name}', GID: {gid}, Rows: {len(mapped_rows)}")
        
        requests.post(
            GOOGLE_SCRIPT_URL,
            json=final_payload,
            timeout=60
        )

        # ----------------------------------------
        # 3️⃣ Update daily_sent count
        # ----------------------------------------
        sent_now = len(mapped_rows)
        cur.execute("""
            UPDATE saved_settings
            SET daily_sent = daily_sent + %s,
                last_sent_date = %s
            WHERE id = %s
        """, (sent_now, today, setting_id))
        conn.commit()

        print(f"✅ Sent {sent_now} leads (Total Today: {daily_sent + sent_now})")

    except Exception as e:
        print("❌ Auto Trigger Error:", e)

    cur.close()
    conn.close()



if __name__ == "__main__":
    app.run(port=5000, debug=True)