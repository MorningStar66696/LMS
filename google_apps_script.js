function doPost(e) {
    var lock = LockService.getScriptLock();
    lock.tryLock(30000);

    try {
        // -------------------------------------------------------------
        // 1️⃣ PARSE DATA
        // We try to read from JSON body first, then fallback to parameters
        // -------------------------------------------------------------
        var postData = {};
        if (e.postData && e.postData.contents) {
            try { postData = JSON.parse(e.postData.contents); } catch (err) { }
        }
        var params = e.parameter || {};

        // -------------------------------------------------------------
        // 2️⃣ READ CONTROL SHEET MODES
        // -------------------------------------------------------------
        if (params.mode === "readControlSheet") {
            return handleReadControl(params.sheetUrl, "Control Sheet");
        }
        if (params.mode === "readSpecificSheet") {
            return handleReadControl(params.sheetUrl, params.sheetName);
        }

        // -------------------------------------------------------------
        // 3️⃣ MAIN LEAD SENDING LOGIC
        // -------------------------------------------------------------
        // Priority: JSON Body > URL Parameters
        const sheetUrl = postData.sheetUrl || params.sheetUrl;

        // ✅ CRITICAL: This is what fixes the "Sheet1" issue. 
        // We look for 'sheetName' or 'tabName' in the data you sent.
        const sheetName = postData.sheetName || postData.tabName || params.sheetName;

        // Handle rows/mappings
        const rows = Array.isArray(postData.rows) ? postData.rows : JSON.parse(params.rows || "[]");
        const mappings = Array.isArray(postData.mappings) ? postData.mappings : JSON.parse(params.mappings || "[]");

        if (!sheetUrl) return respond({ status: "error", message: "Missing sheetUrl" });
        if (!rows || rows.length === 0) return respond({ status: "no_rows", inserted: 0 });

        // Open Spreadsheet
        const ss = SpreadsheetApp.openByUrl(sheetUrl);
        var sheet;

        // ✅ SWITCH TABS LOGIC
        if (sheetName) {
            sheet = ss.getSheetByName(sheetName);
            if (!sheet) {
                // Create if missing? (Optional: uncomment next line)
                // sheet = ss.insertSheet(sheetName);
                return respond({ status: "error", message: "Sheet/Tab '" + sheetName + "' not found in Google Sheet." });
            }
        } else {
            // Default fallback
            sheet = ss.getSheets()[0];
        }

        // -------------------------------------------------------------
        // 4️⃣ APPEND LOGIC (Header Matching & Deduping)
        // -------------------------------------------------------------
        const rawHeaders = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
        const headers = rawHeaders.map(h => String(h).trim().toLowerCase());
        const headerIndex = {};
        headers.forEach((h, i) => (headerIndex[h] = i));

        // Dedupe setup
        const dedupeFields = mappings.filter(m => m.dedupe === true).map(m => m.sheetField.trim().toLowerCase());
        const existingData = sheet.getLastRow() > 1
            ? sheet.getRange(2, 1, sheet.getLastRow() - 1, headers.length).getValues()
            : [];

        const dedupeSets = {};
        dedupeFields.forEach(f => {
            const idx = headerIndex[f];
            if (idx !== undefined) {
                dedupeSets[f] = new Set(existingData.map(r => String(r[idx]).trim()));
            }
        });

        const finalRows = [];
        rows.forEach(obj => {
            const normalized = {};
            Object.keys(obj).forEach(k => normalized[k.trim().toLowerCase()] = String(obj[k]).trim());

            // Check Duplicates
            for (const f of dedupeFields) {
                if (dedupeSets[f] && normalized[f] && dedupeSets[f].has(normalized[f])) return;
            }

            // Map to Header Order
            // If a header exists in the sheet but not in data, it gets empty string
            finalRows.push(headers.map(h => normalized[h] || ""));
        });

        // Write to Sheet
        if (finalRows.length > 0) {
            sheet.getRange(sheet.getLastRow() + 1, 1, finalRows.length, headers.length).setValues(finalRows);
        }

        return respond({
            status: "ok",
            targetSheet: sheet.getName(),
            inserted: finalRows.length,
            skipped: rows.length - finalRows.length
        });

    } catch (err) {
        return respond({ status: "error", message: err.toString() });
    } finally {
        lock.releaseLock();
    }
}

// -------------------------------------------------------------
// HELPER FUNCTIONS
// -------------------------------------------------------------
function handleReadControl(url, tabName) {
    try {
        const ss = SpreadsheetApp.openByUrl(url);
        const sheet = ss.getSheetByName(tabName);
        if (!sheet) return respond({ error: "Sheet not found: " + tabName });

        const data = sheet.getDataRange().getValues();
        const headers = data[0];
        const rows = data.slice(1).map(r => {
            const obj = {};
            headers.forEach((h, i) => obj[String(h).trim()] = r[i]);
            return obj;
        });
        return respond(rows);
    } catch (e) { return respond({ error: e.toString() }); }
}

function respond(obj) {
    return ContentService.createTextOutput(JSON.stringify(obj))
        .setMimeType(ContentService.MimeType.JSON);
}
