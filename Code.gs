// Code.gs
function runGenerateSchedule() {
  const ss = SpreadsheetApp.getActive();
  const repo = new SheetRepo(ss);

  const month = repo.getConfigMonth();
  const people = repo.getPeople();               // [{empId,name,active}]
  const fixed = repo.getFixedShifts();           // [{empId, dateFrom, dateTo, shiftCode}]
  const leave = repo.getLeaves();                // [{empId, date, leaveType}]
  const demand = repo.getDemand();               // [{date, shiftCode, required}]
  const shiftDefs = repo.getShiftTemplate();     // map shiftCode -> def

  const scheduler = new Scheduler(people, fixed, leave, demand, shiftDefs);
  scheduler.buildMonthPlan(month);
  const { headers, matrix } = scheduler.toMonthMatrix();

  repo.writeMonthSchedule(matrix, headers);
}
