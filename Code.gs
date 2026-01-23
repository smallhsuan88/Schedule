// Code.gs
function runGenerateSchedule() {
  const ss = SpreadsheetApp.getActive();
  const repo = new SheetRepo(ss);

  const monthStr = repo.getConfigMonth();
  const people = repo.getPeople().filter(p => p.active); // [{empId,name,active}]
  const fixed = repo.getFixedShifts();                   // [{empId, dateFrom, dateTo, shiftCode}]
  const leave = repo.getLeaves();                        // [{empId, date, leaveType}]
  const lastSnapshot = repo.getLastMonthSnapshot(monthStr);
  const { headers, matrix } = buildMonthScheduleMatrix(monthStr, people, fixed, leave, lastSnapshot);

  repo.writeMonthSchedule(matrix, headers);
}

function buildMonthScheduleMatrix(monthStr, people, fixed, leave, lastSnapshot) {
  if (!monthStr) throw new Error("Config month is required");
  const [yearStr, monthOnlyStr] = String(monthStr).split("-");
  const year = Number(yearStr);
  const monthIndex = Number(monthOnlyStr) - 1;
  if (!year || Number.isNaN(monthIndex)) {
    throw new Error(`Invalid month format: ${monthStr}`);
  }

  const monthStartDate = new Date(year, monthIndex, 1);
  const monthEndDate = new Date(year, monthIndex + 1, 0);
  const prevMonthEndDate = new Date(year, monthIndex, 0);
  const prevMonthStartDate = new Date(year, monthIndex - 1, 1);
  const prevMonthStartDay = 26;

  const dates = [];
  for (let day = prevMonthStartDay; day <= prevMonthEndDate.getDate(); day += 1) {
    dates.push(new Date(prevMonthStartDate.getFullYear(), prevMonthStartDate.getMonth(), day));
  }
  for (let day = 1; day <= monthEndDate.getDate(); day += 1) {
    dates.push(new Date(year, monthIndex, day));
  }

  const dateKeys = dates.map(date => fmtDate(date));
  const dateIndex = new Map(dateKeys.map((key, idx) => [key, idx]));
  const dowRow = ["empId", "name", ...dates.map(d => "日一二三四五六"[dow_(d)])];
  const dayRow = ["empId", "name", ...dates.map(d => d.getDate())];
  const headers = [dowRow, dayRow];

  const scheduleByEmp = new Map();
  for (const person of people) {
    scheduleByEmp.set(person.empId, new Array(dateKeys.length).fill(""));
  }

  for (const [empId, lastByDate] of lastSnapshot.entries()) {
    if (!scheduleByEmp.has(empId)) continue;
    const row = scheduleByEmp.get(empId);
    for (const [dateKey, code] of lastByDate.entries()) {
      const idx = dateIndex.get(dateKey);
      if (idx === undefined) continue;
      row[idx] = code;
    }
  }

  for (const rule of fixed) {
    if (!scheduleByEmp.has(rule.empId)) continue;
    const row = scheduleByEmp.get(rule.empId);
    const datesInRange = expandDateRange(rule.dateFrom, rule.dateTo);
    for (const dt of datesInRange) {
      const dateKey = fmtDate(dt);
      const idx = dateIndex.get(dateKey);
      if (idx === undefined) continue;
      row[idx] = rule.shiftCode;
    }
  }

  for (const entry of leave) {
    if (!scheduleByEmp.has(entry.empId)) continue;
    const row = scheduleByEmp.get(entry.empId);
    const dateKey = fmtDate(entry.date);
    const idx = dateIndex.get(dateKey);
    if (idx === undefined) continue;
    row[idx] = entry.leaveType || "";
  }

  const matrix = people.map(person => {
    const row = scheduleByEmp.get(person.empId) || [];
    return [person.empId, person.name, ...row];
  });

  return { headers, matrix };
}
