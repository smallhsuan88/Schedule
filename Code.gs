// Code.gs
function runGenerateSchedule() {
  const ss = SpreadsheetApp.getActive();
  const repo = new SheetRepo(ss);

  const monthStr = repo.getConfigMonth();
  const people = repo.getPeople();               // [{empId,name,active}]
  const fixed = repo.getFixedShifts();           // [{empId, dateFrom, dateTo, shiftCode}]
  const leave = repo.getLeaves();                // [{empId, date, leaveType}]
  const demand = repo.getDemand();               // [{date, shiftCode, required}]
  const shiftDefs = repo.getShiftTemplate();     // map shiftCode -> def

  const scheduler = new Scheduler(people, fixed, leave, demand, shiftDefs);
  const [yearStr, monthOnlyStr] = monthStr.split("-");
  const monthStartDate = new Date(Number(yearStr), Number(monthOnlyStr) - 1, 1);
  const monthEndDate = new Date(Number(yearStr), Number(monthOnlyStr), 0);
  const windowDays = 7;
  const windowStartDate = new Date(monthStartDate);
  windowStartDate.setDate(windowStartDate.getDate() - windowDays);
  const windowEndDate = new Date(monthEndDate);
  windowEndDate.setDate(windowEndDate.getDate() + windowDays);
  const seedMatrix = repo.getExistingSchedule(windowStartDate, windowEndDate);
  scheduler.buildMonthPlan(monthStr, { seedMatrix, windowDays });
  const { headers, matrix } = scheduler.toMonthMatrix();

  repo.writeMonthSchedule(matrix, headers);
}
