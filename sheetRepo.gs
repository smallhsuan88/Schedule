// sheetRepo.gs (示意：你可以依你表頭調整)
class SheetRepo {
  constructor(ss){ this.ss = ss; }

  getPeople(){ return this.read_("People").map(r => ({
    empId: String(r.empId), name: r.name, active: (r.active === "Y")
  }));}

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
    sh.clearContents();
    const headers = ["date","shiftCode","empId","status"];
    sh.getRange(1,1,1,headers.length).setValues([headers]);
    const data = rows.map(r => [r.date, r.shiftCode, r.empId, r.status]);
    if (data.length) sh.getRange(2,1,data.length,headers.length).setValues(data);
  }

  writeMonthSchedule(matrix, headers){
    const sh = this.ss.getSheetByName("MonthSchedule") || this.ss.insertSheet("MonthSchedule");
    sh.clearContents();
    sh.getRange(1,1,1,headers.length).setValues([headers]);
    if (matrix.length) sh.getRange(2,1,matrix.length,headers.length).setValues(matrix);
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
}
