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
  const windowStartDate = new Date(monthStartDate);
  windowStartDate.setDate(windowStartDate.getDate() - windowStartDate.getDay());
  const windowEndDate = new Date(windowStartDate);
  windowEndDate.setDate(windowEndDate.getDate() + 34);
  const seedMatrix = repo.getLastSchedule(windowStartDate, windowEndDate);
  const existingMatrix = repo.getExistingSchedule(windowStartDate, windowEndDate);
  scheduler.buildMonthPlan(monthStr, { seedMatrix, existingMatrix });
  const { headers, matrix } = scheduler.toMonthMatrix();

  repo.writeMonthSchedule(matrix, headers);
}
