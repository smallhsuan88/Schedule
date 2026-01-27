// Code.gs
function runGenerateSchedule() {
  const ss = SpreadsheetApp.getActive();
  const repo = new SheetRepo(ss);

  const monthStr = repo.getConfigMonth();
  const people = repo.getPeople().filter(p => p.active); // [{empId,name,active}]
  const fixed = repo.getFixedShifts();                   // [{empId, dateFrom, dateTo, shiftCode}]
  const leave = repo.getLeaves();                        // [{empId, date, leaveType}]
  const lastSnapshot = repo.getLastMonthSnapshot(monthStr);
  const shiftDefs = repo.getShiftTemplate();
  const { headers, matrix } = buildMonthScheduleMatrix(monthStr, people, fixed, leave, lastSnapshot, shiftDefs);

  repo.writeMonthSchedule(matrix, headers);
}

function buildMonthScheduleMatrix(monthStr, people, fixed, leave, lastSnapshot, shiftDefs) {
  if (!monthStr) throw new Error("Config month is required");
  const [yearStr, monthOnlyStr] = String(monthStr).split("-");
  const year = Number(yearStr);
  const monthIndex = Number(monthOnlyStr) - 1;
  if (!year || Number.isNaN(monthIndex)) {
    throw new Error(`Invalid month format: ${monthStr}`);
  }

  const monthEndDate = new Date(year, monthIndex + 1, 0);
  const requiredStaff = 3;
  const maxOff = Math.max(0, people.length - requiredStaff);

  const dates = [];
  const windowStart = new Date(monthEndDate);
  windowStart.setDate(windowStart.getDate() - 34);
  for (let i = 0; i < 35; i += 1) {
    const date = new Date(windowStart);
    date.setDate(windowStart.getDate() + i);
    dates.push(date);
  }
  if (dates.length !== 35) {
    throw new Error(`dates length must be 35, got ${dates.length}`);
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
  Logger.log(`[INFO] dates.length=${dates.length} sunDays/satDays will be computed`);

  const leaveSet = new Set(leave.map(entry => `${entry.empId}#${fmtDate(entry.date)}`));
  const windowDateKeySet = new Set(dateKeys);

  // Step A4: 回填 LAST (window-based)
  for (const [empId, lastByDate] of lastSnapshot.entries()) {
    if (!scheduleByEmp.has(empId)) continue;
    const row = scheduleByEmp.get(empId);
    for (const [dateKey, code] of lastByDate.entries()) {
      const idx = dateIndex.get(dateKey);
      if (idx === undefined || !windowDateKeySet.has(dateKey)) continue;
      row[idx] = code;
    }
  }

  // Step A5: Leave 覆蓋
  for (const entry of leave) {
    if (!scheduleByEmp.has(entry.empId)) continue;
    const row = scheduleByEmp.get(entry.empId);
    const dateKey = fmtDate(entry.date);
    const idx = dateIndex.get(dateKey);
    if (idx === undefined) continue;
    row[idx] = entry.leaveType || "";
  }

  const SHIFT_CODES = Object.freeze({
    REST_SUN: "R_sun",
    REST: "r",
    EARLY: "早L",
    NOON: "午L",
    NIGHT: "夜L",
    FIXED_NIGHT: "常夜L"
  });

  const isRestCode = code => code === SHIFT_CODES.REST_SUN || code === SHIFT_CODES.REST;
  const isNightCode = code => code === SHIFT_CODES.NIGHT || code === SHIFT_CODES.FIXED_NIGHT;
  const isEarlyCode = code => code === SHIFT_CODES.EARLY;
  const isNoonCode = code => code === SHIFT_CODES.NOON;

  const isLeave = (empId, dateKey) => leaveSet.has(`${empId}#${dateKey}`);
  const getCell = (empId, dateKey) => {
    const row = scheduleByEmp.get(empId) || [];
    const idx = dateIndex.get(dateKey);
    return idx === undefined ? "" : row[idx];
  };
  const setCell = (empId, dateKey, code) => {
    const row = scheduleByEmp.get(empId) || [];
    const idx = dateIndex.get(dateKey);
    if (idx === undefined) return;
    row[idx] = code;
  };

  const offCountByDate = new Map(dateKeys.map(key => [key, 0]));
  for (const dateKey of dateKeys) {
    let offCount = 0;
    for (const person of people) {
      const code = getCell(person.empId, dateKey);
      if (isLeave(person.empId, dateKey) || isRestCode(code)) offCount += 1;
    }
    offCountByDate.set(dateKey, offCount);
  }
  const rSunCountByDate = new Map(dateKeys.map(key => [key, 0]));

  // Step B1: 找出週日/週六
  const sunDays = dates.filter(date => dow_(date) === 0).map(date => fmtDate(date));
  const satDays = dates.filter(date => dow_(date) === 6).map(date => fmtDate(date));
  Logger.log(`[INFO] dates.length=${dates.length} sunDays=${sunDays.length} satDays=${satDays.length}`);
  if (sunDays.length !== 5 || satDays.length !== 5) {
    throw new Error(`sunDays/satDays must be 5/5. got sun=${sunDays.length} sat=${satDays.length}`);
  }

  const canAssignOff = (empId, dateKey, code) => {
    if (isLeave(empId, dateKey)) return false;
    const existing = getCell(empId, dateKey);
    if (existing) return false;
    const offCount = offCountByDate.get(dateKey) || 0;
    if (offCount + 1 > maxOff) return false;
    if (code === SHIFT_CODES.REST_SUN) {
      const count = rSunCountByDate.get(dateKey) || 0;
      if (count + 1 > 1) return false;
    }
    return true;
  };
  const assignOff = (empId, dateKey, code) => {
    if (!canAssignOff(empId, dateKey, code)) return false;
    setCell(empId, dateKey, code);
    offCountByDate.set(dateKey, (offCountByDate.get(dateKey) || 0) + 1);
    if (code === SHIFT_CODES.REST_SUN) {
      rSunCountByDate.set(dateKey, (rSunCountByDate.get(dateKey) || 0) + 1);
    }
    return true;
  };

  const rSunByEmp = new Map();
  // Step B2: 指派 R_sun
  people.forEach((person, personIdx) => {
    const assigned = [];
    for (let i = 0; i < 5; i += 1) {
      let placed = false;
      for (let attempt = 0; attempt < sunDays.length; attempt += 1) {
        const sunIdx = (personIdx + i + attempt) % sunDays.length;
        const dateKey = sunDays[sunIdx];
        if (assigned.includes(dateKey)) continue;
        if (assignOff(person.empId, dateKey, SHIFT_CODES.REST_SUN)) {
          assigned.push(dateKey);
          placed = true;
          break;
        }
      }
      if (!placed) {
        Logger.log(`[ERROR] R_sun assignment failed empId=${person.empId} attempt=${i}`);
        throw new Error(`R_sun assignment failed: empId=${person.empId}`);
      }
    }
    rSunByEmp.set(person.empId, assigned);
  });

  const rByEmp = new Map();
  // Step B3: 指派 r
  people.forEach(person => {
    const assigned = [];
    const rSunDates = rSunByEmp.get(person.empId) || [];
    const preferred = [];
    rSunDates.forEach(dateKey => {
      const idx = dateIndex.get(dateKey);
      const prevKey = dateKeys[idx - 1];
      const nextKey = dateKeys[idx + 1];
      if (prevKey) preferred.push(prevKey);
      if (nextKey) preferred.push(nextKey);
    });
    const fallback = [...satDays, ...dateKeys];
    const candidateDays = [...preferred, ...fallback];
    for (const dateKey of candidateDays) {
      if (assigned.length >= 5) break;
      if (assigned.includes(dateKey)) continue;
      if (assignOff(person.empId, dateKey, SHIFT_CODES.REST)) {
        assigned.push(dateKey);
      }
    }
    if (assigned.length !== 5) {
      Logger.log(`[ERROR] r assignment failed empId=${person.empId} count=${assigned.length}`);
      throw new Error(`r assignment failed: empId=${person.empId} count=${assigned.length}`);
    }
    rByEmp.set(person.empId, assigned);
  });

  for (const person of people) {
    const rSunCount = (rSunByEmp.get(person.empId) || []).length;
    const rCount = (rByEmp.get(person.empId) || []).length;
    Logger.log(`[INFO] empId=${person.empId} R_sun=${rSunCount} r=${rCount}`);
  }

  for (const dateKey of dateKeys) {
    const offCount = offCountByDate.get(dateKey) || 0;
    Logger.log(`[INFO] date=${dateKey} offCount=${offCount}`);
    if (offCount > maxOff) {
      throw new Error(`offCount exceeded: date=${dateKey} offCount=${offCount}`);
    }
  }

  // Step C: 常夜固定班
  const fixedNightEmpSet = new Set();
  for (const rule of fixed) {
    if (String(rule.shiftCode || "").trim() !== SHIFT_CODES.FIXED_NIGHT) continue;
    fixedNightEmpSet.add(rule.empId);
    const datesInRange = expandDateRange(rule.dateFrom, rule.dateTo);
    for (const dt of datesInRange) {
      const dateKey = fmtDate(dt);
      if (!dateIndex.has(dateKey)) continue;
      if (isLeave(rule.empId, dateKey)) continue;
      const existing = getCell(rule.empId, dateKey);
      if (isRestCode(existing)) continue;
      if (existing) continue;
      setCell(rule.empId, dateKey, SHIFT_CODES.FIXED_NIGHT);
    }
  }

  // Step D: 補夜L、早L、午L
  for (const dateKey of dateKeys) {
    const assignedEmpIds = new Set();
    let nightCount = 0;
    let earlyCount = 0;
    let noonCount = 0;
    for (const person of people) {
      const code = getCell(person.empId, dateKey);
      if (!code) continue;
      assignedEmpIds.add(person.empId);
      if (isNightCode(code)) nightCount += 1;
      if (isEarlyCode(code)) earlyCount += 1;
      if (isNoonCode(code)) noonCount += 1;
    }

    const available = people
      .filter(p => !fixedNightEmpSet.has(p.empId))
      .filter(p => !assignedEmpIds.has(p.empId))
      .filter(p => !isLeave(p.empId, dateKey))
      .filter(p => !isRestCode(getCell(p.empId, dateKey)))
      .map(p => p.empId);

    if (nightCount === 0) {
      const pick = available.shift();
      if (!pick) {
        throw new Error(`nightL assign failed: date=${dateKey}`);
      }
      setCell(pick, dateKey, SHIFT_CODES.NIGHT);
      assignedEmpIds.add(pick);
      nightCount += 1;
    }

    const remaining = available.filter(empId => !assignedEmpIds.has(empId));
    if (earlyCount === 0) {
      const pick = remaining.shift();
      if (!pick) {
        throw new Error(`earlyL assign failed: date=${dateKey}`);
      }
      setCell(pick, dateKey, SHIFT_CODES.EARLY);
      assignedEmpIds.add(pick);
      earlyCount += 1;
    }

    const remainingAfterEarly = remaining.filter(empId => !assignedEmpIds.has(empId));
    if (noonCount === 0) {
      const pick = remainingAfterEarly.shift();
      if (!pick) {
        throw new Error(`noonL assign failed: date=${dateKey}`);
      }
      setCell(pick, dateKey, SHIFT_CODES.NOON);
      assignedEmpIds.add(pick);
      noonCount += 1;
    }

    Logger.log(`[INFO] date=${dateKey} early=${earlyCount} noon=${noonCount} night=${nightCount}`);
    if (earlyCount !== 1 || noonCount !== 1 || nightCount !== 1) {
      throw new Error(`daily coverage failed: date=${dateKey} early=${earlyCount} noon=${noonCount} night=${nightCount}`);
    }
  }

  // Step E: 11 小時檢核 + swap
  const parseTime = value => {
    if (value instanceof Date && !isNaN(value.getTime())) {
      return value.getHours() * 60 + value.getMinutes();
    }
    if (typeof value === "number" && !Number.isNaN(value)) {
      return Math.round(value * 24 * 60);
    }
    if (typeof value === "string" && value.includes(":")) {
      const [h, m] = value.split(":").map(Number);
      if (!Number.isNaN(h) && !Number.isNaN(m)) return h * 60 + m;
    }
    return null;
  };

  const getShiftWindow = (dateKey, code) => {
    const def = shiftDefs[code];
    if (!def) return null;
    const startMinutes = parseTime(def.startTime);
    const endMinutes = parseTime(def.endTime);
    if (startMinutes === null || endMinutes === null) return null;
    const start = new Date(dateKey);
    start.setHours(Math.floor(startMinutes / 60), startMinutes % 60, 0, 0);
    const end = new Date(dateKey);
    end.setHours(Math.floor(endMinutes / 60), endMinutes % 60, 0, 0);
    if (endMinutes < startMinutes) end.setDate(end.getDate() + 1);
    return { start, end };
  };

  const isWorkShift = code => code && shiftDefs[code];

  const checkRestHours = (prevDateKey, prevCode, nextDateKey, nextCode) => {
    if (!nextDateKey) return true;
    if (!isWorkShift(prevCode) || !isWorkShift(nextCode)) return true;
    const prev = getShiftWindow(prevDateKey, prevCode);
    const next = getShiftWindow(nextDateKey, nextCode);
    if (!prev || !next) return true;
    const diffHours = (next.start.getTime() - prev.end.getTime()) / 3600000;
    return diffHours >= 11;
  };

  for (const person of people) {
    for (let i = 1; i < dateKeys.length; i += 1) {
      const prevDateKey = dateKeys[i - 1];
      const dateKey = dateKeys[i];
      const prevCode = getCell(person.empId, prevDateKey);
      const code = getCell(person.empId, dateKey);
      if (!isWorkShift(prevCode) || !isWorkShift(code)) continue;
      if (checkRestHours(prevDateKey, prevCode, dateKey, code)) continue;

      let swapped = false;
      for (const peer of people) {
        if (peer.empId === person.empId) continue;
        if (fixedNightEmpSet.has(peer.empId)) continue;
        const peerCode = getCell(peer.empId, dateKey);
        if (!isWorkShift(peerCode)) continue;
        const personNextCode = getCell(person.empId, dateKeys[i + 1]);
        const peerPrevCode = getCell(peer.empId, dateKeys[i - 1]);
        const peerNextCode = getCell(peer.empId, dateKeys[i + 1]);
        const personOk = checkRestHours(prevDateKey, prevCode, dateKey, peerCode)
          && checkRestHours(dateKey, peerCode, dateKeys[i + 1], personNextCode);
        const peerOk = checkRestHours(dateKeys[i - 1], peerPrevCode, dateKey, code)
          && checkRestHours(dateKey, code, dateKeys[i + 1], peerNextCode);
        if (!personOk || !peerOk) continue;
        setCell(person.empId, dateKey, peerCode);
        setCell(peer.empId, dateKey, code);
        swapped = true;
        break;
      }
      if (!swapped) {
        const prevWindow = getShiftWindow(prevDateKey, prevCode);
        const currWindow = getShiftWindow(dateKey, code);
        const diffHours = prevWindow && currWindow
          ? (currWindow.start.getTime() - prevWindow.end.getTime()) / 3600000
          : "unknown";
        throw new Error(`11h violation: empId=${person.empId} date=${dateKey} prev=${prevCode} curr=${code} hours=${diffHours}`);
      }
    }
  }

  const matrix = people.map(person => {
    const row = scheduleByEmp.get(person.empId) || [];
    return [person.empId, person.name, ...row];
  });

  return { headers, matrix };
}
