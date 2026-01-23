// sheetRepo.gs (示意：你可以依你表頭調整)
class SheetRepo {
  constructor(ss){ this.ss = ss; }

  getPeople(){
    const rows = this.read_("People");
    const activeKey = Object.keys(rows[0] || {}).find(k => String(k).toLowerCase().includes("active")) || "active";
    const pickNumber = (row, keys) => {
      for (const key of keys) {
        if (row[key] === "" || row[key] === null || row[key] === undefined) continue;
        const value = Number(row[key]);
        if (!Number.isNaN(value)) return value;
      }
      return null;
    };
    return rows.map(r => ({
      empId: String(r.empId),
      name: r.name,
      active: (r[activeKey] === "Y"),
      quotaSun: pickNumber(r, ["quotaSun", "quota_sun", "R_sun", "sunQuota"]),
      quotaSat: pickNumber(r, ["quotaSat", "quota_sat", "R_sat", "satQuota"]),
      quotaOffTotal: pickNumber(r, ["quotaOffTotal", "quota_off_total", "offQuota", "offTotal"]),
      monthlyOff: pickNumber(r, ["monthlyOff", "monthOff", "offDays"])
    }));
  }

  getFixedShifts(){ return this.read_("FixedShift").map(r => ({
    empId: String(r.empId), dateFrom: r.dateFrom, dateTo: r.dateTo, shiftCode: r.fixedShiftCode
  }));}

  getLeaves(){ return this.read_("Leave").map(r => ({
    empId: String(r.empId), date: r.date, leaveType: r.leaveType
  }));}

  getDemand(){ return this.read_("Demand").map(r => ({
    date: r.date, shiftCode: r.shiftCode, required: Number(r.requiredHeadcount || 0)
  }));}

  getShiftTemplate(){
    const rows = this.read_("ShiftTemplate");
    const map = {};
    rows.forEach(r => map[r.shiftCode] = r);
    return map;
  }

  getConfigMonth(){
    const sh = this.ss.getSheetByName("Config");
    if (!sh) throw new Error("Missing sheet: Config");

    const v = sh.getRange("B1").getValue(); // could be Date or string
    if (v === "" || v === null) throw new Error("Config!B1 is empty");

    // Date object -> yyyy-MM
    if (Object.prototype.toString.call(v) === "[object Date]" && !isNaN(v.getTime())) {
      return Utilities.formatDate(v, "Asia/Taipei", "yyyy-MM");
    }

    const s = String(v).trim();

    // YYYY-MM
    if (/^\d{4}-\d{2}$/.test(s)) return s;

    // YYYY/MM
    if (/^\d{4}\/\d{2}$/.test(s)) return s.replace("/", "-");

    // YYYY-MM-DD or YYYY/MM/DD
    const m = s.match(/^(\d{4})[-\/](\d{1,2})[-\/](\d{1,2})$/);
    if (m) {
      const yyyy = m[1];
      const mm = String(m[2]).padStart(2, "0");
      return `${yyyy}-${mm}`;
    }

    // Last resort: parse date-like strings
    const d = new Date(s);
    if (!isNaN(d.getTime())) {
      return Utilities.formatDate(d, "Asia/Taipei", "yyyy-MM");
    }

    throw new Error("Invalid month format in Config!B1. Use YYYY-MM (e.g., 2026-01) or a date within that month.");
  }

  writeSchedule(rows){
    const sh = this.ss.getSheetByName("Schedule") || this.ss.insertSheet("Schedule");
    const headers = ["date","shiftCode","empId","status"];
    const values = [headers, ...rows.map(row => [row.date, row.shiftCode, row.empId, row.status])];
    const targetRange = sh.getRange(1, 1, values.length, headers.length);
    targetRange.clearContent();
    targetRange.setValues(values);
  }

  writeMonthSchedule(matrix, headers){
    const sh = this.ss.getSheetByName("MonthSchedule") || this.ss.insertSheet("MonthSchedule");
    const headerRows = Array.isArray(headers[0]) ? headers : [headers];
    const totalRows = headerRows.length + matrix.length;
    const totalCols = headerRows[0].length;
    const values = headerRows.concat(matrix);
    const targetRange = sh.getRange(1, 1, totalRows, totalCols);
    targetRange.clearContent();
    this.validateRangeValues_(targetRange, values, "MonthSchedule");
    targetRange.setValues(values);
  }

  writeLastSchedule(matrix, headers){
    const sh = this.ss.getSheetByName("LAST") || this.ss.insertSheet("LAST");
    const headerRows = Array.isArray(headers[0]) ? headers : [headers];
    const totalRows = headerRows.length + matrix.length;
    const totalCols = headerRows[0].length;
    const existingValues = sh.getRange(1, 1, totalRows, totalCols).getValues();

    for (let r = 0; r < headerRows.length; r += 1) {
      for (let c = 0; c < headerRows[r].length; c += 1) {
        sh.getRange(r + 1, c + 1).setValue(headerRows[r][c]);
      }
    }

    for (let r = 0; r < matrix.length; r += 1) {
      for (let c = 0; c < matrix[r].length; c += 1) {
        const targetRow = r + headerRows.length;
        const existing = existingValues[targetRow][c];
        if (existing === "" || existing === null) {
          sh.getRange(targetRow + 1, c + 1).setValue(matrix[r][c]);
        }
      }
    }
  }

  getLastSchedule(windowStartDate, windowEndDate){
    const sh = this.ss.getSheetByName("LAST");
    const seedMatrix = new Map();
    if (!sh) return seedMatrix;
    const values = sh.getDataRange().getValues();
    if (values.length < 3) return seedMatrix;
    const windowStart = new Date(windowStartDate);
    const windowEnd = new Date(windowEndDate);
    const maxCols = 2 + Math.round((windowEnd.getTime() - windowStart.getTime()) / 86400000) + 1;
    for (let rowIdx = 2; rowIdx < values.length; rowIdx += 1) {
      const row = values[rowIdx];
      const empId = String(row[0] || "").trim();
      if (!empId) continue;
      if (!seedMatrix.has(empId)) seedMatrix.set(empId, new Map());
      const byDate = seedMatrix.get(empId);
      for (let colIdx = 2; colIdx < row.length && colIdx < maxCols; colIdx += 1) {
        const offset = colIdx - 2;
        const date = new Date(windowStart);
        date.setDate(date.getDate() + offset);
        if (date < windowStart || date > windowEnd) continue;
        const code = row[colIdx];
        if (code === "" || code === null) continue;
        byDate.set(Utilities.formatDate(date, "Asia/Taipei", "yyyy-MM-dd"), code);
      }
    }
    return seedMatrix;
  }

  getExistingSchedule(windowStartDate, windowEndDate){
    const windowStart = new Date(windowStartDate);
    const windowEnd = new Date(windowEndDate);
    const monthStarts = [];
    for (let dt = new Date(windowStart.getFullYear(), windowStart.getMonth(), 1);
      dt <= windowEnd;
      dt.setMonth(dt.getMonth() + 1)) {
      monthStarts.push(new Date(dt.getFullYear(), dt.getMonth(), 1));
    }
    const midDate = new Date(windowStart.getTime() + (windowEnd.getTime() - windowStart.getTime()) / 2);
    const midMonthStart = new Date(midDate.getFullYear(), midDate.getMonth(), 1);
    const seedMatrix = new Map();
    const candidateSheets = this.ss.getSheets().filter(sh => {
      const name = sh.getName();
      return name === "MonthSchedule" || name.startsWith("MonthSchedule_");
    });
    for (const sh of candidateSheets) {
      const sheetName = sh.getName();
      let baseMonth = null;
      const suffixMatch = sheetName.match(/^MonthSchedule_(\d{4})-(\d{2})$/);
      if (suffixMatch) {
        baseMonth = new Date(Number(suffixMatch[1]), Number(suffixMatch[2]) - 1, 1);
      }
      const values = sh.getDataRange().getValues();
      if (values.length < 3) continue;
      const headerRow = values[0] || [];
      const secondRow = values[1] || [];
      const dayRow = Number(secondRow[2]) ? secondRow : headerRow;
      const dayNumbers = dayRow.slice(2).map(v => Number(v)).filter(v => !Number.isNaN(v));
      if (!baseMonth) {
        const midDaysInMonth = new Date(midMonthStart.getFullYear(), midMonthStart.getMonth() + 1, 0).getDate();
        if (dayNumbers.length === midDaysInMonth) {
          baseMonth = midMonthStart;
        } else {
          const monthCandidates = monthStarts.filter(monthStart => {
            const daysInMonth = new Date(monthStart.getFullYear(), monthStart.getMonth() + 1, 0).getDate();
            return dayNumbers.length === daysInMonth;
          });
          baseMonth = monthCandidates[0] || null;
        }
      }
      if (!baseMonth) continue;
      const year = baseMonth.getFullYear();
      const month = baseMonth.getMonth();
      for (let rowIdx = 2; rowIdx < values.length; rowIdx += 1) {
        const row = values[rowIdx];
        const empId = String(row[0] || "").trim();
        if (!empId) continue;
        if (!seedMatrix.has(empId)) seedMatrix.set(empId, new Map());
        const byDate = seedMatrix.get(empId);
        for (let colIdx = 2; colIdx < row.length && colIdx - 2 < dayNumbers.length; colIdx += 1) {
          const dayNum = dayNumbers[colIdx - 2];
          if (!dayNum) continue;
          const date = new Date(year, month, dayNum);
          if (date < windowStart || date > windowEnd) continue;
          const code = row[colIdx];
          if (code === "" || code === null) continue;
          byDate.set(Utilities.formatDate(date, "Asia/Taipei", "yyyy-MM-dd"), code);
        }
      }
    }
    return seedMatrix;
  }

  read_(sheetName){
    const sh = this.ss.getSheetByName(sheetName);
    if (!sh) throw new Error(`Missing sheet: ${sheetName}`);
    const values = sh.getDataRange().getValues();
    const headers = values[0].map(h => String(h).trim());
    return values.slice(1).filter(r => r.some(v => v !== "")).map(row => {
      const obj = {};
      headers.forEach((h,i)=> obj[h] = row[i]);
      return obj;
    });
  }

  getLastMonthSnapshot(monthStr){
    if (!monthStr) throw new Error("Config month is required");
    const [yearStr, monthOnlyStr] = String(monthStr).split("-");
    const year = Number(yearStr);
    const monthIndex = Number(monthOnlyStr) - 1;
    if (!year || Number.isNaN(monthIndex)) {
      throw new Error(`Invalid month format: ${monthStr}`);
    }
    const prevMonthStart = new Date(year, monthIndex - 1, 1);
    const prevMonthEnd = new Date(year, monthIndex, 0);
    const daysInPrevMonth = prevMonthEnd.getDate();

    const sh = this.ss.getSheetByName("LAST");
    const seedMatrix = new Map();
    if (!sh) return seedMatrix;
    const values = sh.getDataRange().getValues();
    if (values.length < 3) return seedMatrix;

    const headerRow = values[0] || [];
    const secondRow = values[1] || [];
    const dayRow = Number(secondRow[2]) ? secondRow : headerRow;
    const dayNumbers = dayRow.slice(2).map(v => Number(v));
    if (!dayNumbers.some(v => Number.isFinite(v))) return seedMatrix;

    for (let rowIdx = 2; rowIdx < values.length; rowIdx += 1) {
      const row = values[rowIdx];
      const empId = String(row[0] || "").trim();
      if (!empId) continue;
      if (!seedMatrix.has(empId)) seedMatrix.set(empId, new Map());
      const byDate = seedMatrix.get(empId);
      for (let colIdx = 2; colIdx < row.length && colIdx - 2 < dayNumbers.length; colIdx += 1) {
        const dayNum = dayNumbers[colIdx - 2];
        if (!dayNum || dayNum > daysInPrevMonth) continue;
        const date = new Date(prevMonthStart.getFullYear(), prevMonthStart.getMonth(), dayNum);
        const code = row[colIdx];
        if (code === "" || code === null) continue;
        byDate.set(Utilities.formatDate(date, "Asia/Taipei", "yyyy-MM-dd"), code);
      }
    }
    return seedMatrix;
  }

  validateRangeValues_(range, values, context){
    const rangeRows = range.getNumRows();
    const rangeCols = range.getNumColumns();
    const valueRows = values.length;
    const valueCols = valueRows > 0 ? values[0].length : 0;
    const isRect = values.every(row => row.length === valueCols);
    if (!isRect || rangeRows !== valueRows || rangeCols !== valueCols) {
      Logger.log(`[ERROR] ${context} range rows=${rangeRows} cols=${rangeCols} values rows=${valueRows} cols=${valueCols} rectangular=${isRect}`);
      throw new Error(`Range size mismatch for ${context}`);
    }
  }
}
