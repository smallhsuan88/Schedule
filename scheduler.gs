// scheduler.gs
class Scheduler {
  constructor(people, fixed, leave, demand, shiftDefs) {
    this.people = people.filter(p => p.active);
    this.fixed = fixed;
    this.leave = leave;
    this.demand = demand;
    this.shiftDefs = shiftDefs;

    this.leaveSet = new Set(leave.map(x => `${x.empId}#${fmtDate(x.date)}`));
    this.fixedRules = fixed; // 你可以先簡單做，後面再索引加速

    // scheduleMap: key = date#shiftCode -> array empIds
    this.scheduleMap = new Map();

    // empAssigned: key = empId#date -> shiftCode (避免一天多班)
    this.empAssigned = new Map();

    // counter: empId -> assignedCount (用於均衡)
    this.counter = new Map(this.people.map(p => [p.empId, 0]));

    this.monthPlan = null;
  }

  generate() {
    // 1) 先套用固定班
    this.applyFixedShifts();

    // 2) 再依需求補齊
    this.fillByDemand();

    // 3) 組合輸出
    return this.flattenResult();
  }

  applyFixedShifts() {
    for (const rule of this.fixedRules) {
      const dates = expandDateRange(rule.dateFrom, rule.dateTo); // 支援單日或區間
      for (const d of dates) {
        const dateKey = fmtDate(d);

        // 休假優先：若同日有請假，就不排固定班（或標衝突）
        if (this.leaveSet.has(`${rule.empId}#${dateKey}`)) continue;

        // 一天一班檢查
        if (this.empAssigned.has(`${rule.empId}#${dateKey}`)) continue;

        this.assign(rule.empId, dateKey, rule.shiftCode, "fixed");
      }
    }
  }

  fillByDemand() {
    // 依 date / shiftCode 排序，先排滿缺口大的
    const sorted = [...this.demand].sort((a,b) => {
      if (fmtDate(a.date) !== fmtDate(b.date)) return fmtDate(a.date) < fmtDate(b.date) ? -1 : 1;
      return b.required - a.required;
    });

    for (const req of sorted) {
      const dateKey = fmtDate(req.date);
      const key = `${dateKey}#${req.shiftCode}`;

      const already = (this.scheduleMap.get(key) || []).length;
      const need = Math.max(0, req.required - already);
      if (need === 0) continue;

      // 候選人：未請假、未排班、且沒有固定成其他班型的人
      const candidates = this.people
        .map(p => p.empId)
        .filter(empId => !this.leaveSet.has(`${empId}#${dateKey}`))
        .filter(empId => !this.empAssigned.has(`${empId}#${dateKey}`))
        .filter(empId => !this.isFixedToOtherShift(empId, dateKey, req.shiftCode));

      // 簡單公平策略：優先挑目前排班次數少的人
      candidates.sort((a,b) => (this.counter.get(a) || 0) - (this.counter.get(b) || 0));

      for (let i=0; i<need && i<candidates.length; i++) {
        this.assign(candidates[i], dateKey, req.shiftCode, "assigned");
      }

      // 若仍缺人，可在輸出標示 unfilled
      // （或你也可以加「可加班名單」「支援名單」第二層池）
    }
  }

  isFixedToOtherShift(empId, dateKey, targetShift) {
    // MVP：只要該日被固定到某班且不是 targetShift，就視為不可用
    for (const rule of this.fixedRules) {
      if (rule.empId !== empId) continue;
      const dates = expandDateRange(rule.dateFrom, rule.dateTo).map(fmtDate);
      if (dates.includes(dateKey) && rule.shiftCode !== targetShift) return true;
    }
    return false;
  }

  assign(empId, dateKey, shiftCode, status) {
    const key = `${dateKey}#${shiftCode}`;
    const arr = this.scheduleMap.get(key) || [];
    arr.push({ empId, status });
    this.scheduleMap.set(key, arr);

    this.empAssigned.set(`${empId}#${dateKey}`, shiftCode);
    this.counter.set(empId, (this.counter.get(empId) || 0) + 1);
  }

  getDailyOffCap(dateKey, requiredShiftsCount = 3) {
    const peopleCount = this.people.length;
    return Math.max(0, peopleCount - requiredShiftsCount);
  }

  flattenResult() {
    const out = [];
    for (const [key, arr] of this.scheduleMap.entries()) {
      const [dateKey, shiftCode] = key.split("#");
      for (const item of arr) {
        out.push({ date: dateKey, shiftCode, empId: item.empId, status: item.status });
      }
    }
    return out;
  }

  buildMonthPlan(month, options = {}) {
    if (Object.prototype.toString.call(month) === "[object Date]" && !isNaN(month.getTime())) {
      month = Utilities.formatDate(month, "Asia/Taipei", "yyyy-MM");
    }
    if (!month) throw new Error("Config month is required");
    const [yearStr, monthStr] = String(month).split("-");
    const year = Number(yearStr);
    const monthIndex = Number(monthStr) - 1;
    if (!year || Number.isNaN(monthIndex)) {
      throw new Error(`Invalid month format: ${month}`);
    }

    const monthStartDate = new Date(year, monthIndex, 1);
    const monthEndDate = new Date(year, monthIndex + 1, 0);
    const daysInMonth = monthEndDate.getDate();

    const days = [];
    for (let day = 1; day <= daysInMonth; day += 1) {
      const date = new Date(year, monthIndex, day);
      days.push({ day, dateKey: fmtDate(date), date });
    }

    const windowStartDate = new Date(monthStartDate);
    windowStartDate.setDate(windowStartDate.getDate() - windowStartDate.getDay());
    const windowDays = [];
    for (let i = 0; i < 35; i += 1) {
      const dt = new Date(windowStartDate);
      dt.setDate(dt.getDate() + i);
      windowDays.push({ date: dt, dateKey: fmtDate(dt) });
    }

    // Phase 0: calendar structure (window-based)
    const calendarDates = windowDays.map(({ date }) => date);
    const dateKeys = windowDays.map(({ dateKey }) => dateKey);
    const dow = calendarDates.map(date => dow_(date));
    const dayIndex = new Map(windowDays.map((item, idx) => [item.dateKey, idx]));

    const shiftByCode = new Map(Object.values(this.shiftDefs).map(def => [def.shiftCode, def]));

    const SHIFT_TYPES = Object.freeze({
      EARLY: "E",
      NOON: "M",
      NIGHT: "N",
      LONG_NIGHT: "LN",
      OFF: "OFF",
      REST_SUN: "R",
      REST_GENERAL: "r"
    });

    const CELL_STATUS = Object.freeze({
      EMPTY: "EMPTY",
      LOCKED_LEAVE: "LOCKED_LEAVE",
      LOCKED_SEED: "LOCKED_SEED",
      LOCKED_OFF: "LOCKED_OFF",
      LOCKED_LN: "LOCKED_LN",
      LOCKED_N: "LOCKED_N"
    });
    const leaveTypes = new Set(this.leave.map(item => item.leaveType).filter(Boolean));
    const fixedRulesByEmp = new Map();
    for (const rule of this.fixedRules) {
      if (!fixedRulesByEmp.has(rule.empId)) fixedRulesByEmp.set(rule.empId, []);
      fixedRulesByEmp.get(rule.empId).push(rule);
    }

    const parseTime_ = value => {
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

    const getShiftStartEnd = (dateKey, shiftCode) => {
      const def = shiftByCode.get(shiftCode);
      if (!def) return null;
      const startMinutes = parseTime_(def.startTime);
      const endMinutes = parseTime_(def.endTime);
      if (startMinutes === null || endMinutes === null) return null;
      const start = new Date(dateKey);
      start.setHours(Math.floor(startMinutes / 60), startMinutes % 60, 0, 0);
      const end = new Date(dateKey);
      end.setHours(Math.floor(endMinutes / 60), endMinutes % 60, 0, 0);
      if (endMinutes < startMinutes) {
        end.setDate(end.getDate() + 1);
      }
      return { start, end, minRestHours: Number(def.minRestHours) || 11 };
    };

    const coverageShiftCodes = {
      early: shiftByCode.has("早L") ? "早L" : "早",
      noon: shiftByCode.has("午L") ? "午L" : "午",
      night: shiftByCode.has("夜L") ? "夜L" : "夜",
      longNight: shiftByCode.has("常夜L") ? "常夜L" : "常夜"
    };


    const normalizeSeedCode = (dateKey, code) => {
      if (!code) return "";
      if (code === SHIFT_TYPES.REST_SUN || code === SHIFT_TYPES.REST_GENERAL) return code;
      if (code === "R_sun" || code === "R_sat") return SHIFT_TYPES.REST_SUN;
      if (code === "R") {
        const idx = dayIndex.get(dateKey);
        if (idx !== undefined && dow[idx] === 0) return SHIFT_TYPES.REST_SUN;
        return SHIFT_TYPES.REST_GENERAL;
      }
      return code;
    };

    const isLeaveCode_ = code => {
      if (!code) return false;
      if (code === SHIFT_TYPES.REST_SUN || code === SHIFT_TYPES.REST_GENERAL) return false;
      if (code === coverageShiftCodes.early || code === coverageShiftCodes.noon || code === coverageShiftCodes.night || code === coverageShiftCodes.longNight) {
        return false;
      }
      return leaveTypes.has(code) || !shiftByCode.has(code);
    };
    const isOffCode = code => code === SHIFT_TYPES.REST_SUN || code === SHIFT_TYPES.REST_GENERAL || isLeaveCode_(code);
    const isWorkCode = code => Boolean(code) && !isOffCode(code);
    const isNightCode_ = code => code === "夜" || code === "夜L" || String(code || "").includes("常夜");
    const isEarlyCode_ = code => code === "早" || code === "早L";
    const isNoonCode_ = code => code === "午" || code === "午L";

    const plan = new Map();
    const statusPlan = new Map();
    for (const person of this.people) {
      const byDate = new Map();
      const statusByDate = new Map();
      for (const { dateKey } of windowDays) {
        byDate.set(dateKey, []);
        statusByDate.set(dateKey, CELL_STATUS.EMPTY);
      }
      plan.set(person.empId, byDate);
      statusPlan.set(person.empId, statusByDate);
    }

    const weekBucketsIdx = [];
    for (let w = 0; w < 5; w += 1) {
      const bucket = [];
      for (let d = 0; d < 7; d += 1) {
        bucket.push(w * 7 + d);
      }
      weekBucketsIdx.push(bucket);
    }

    const sundayIdx = [];
    for (let idx = 0; idx < windowDays.length; idx += 1) {
      const dayOfWeek = dow[idx];
      if (dayOfWeek === 0) sundayIdx.push(idx);
    }

    const warnings = [];
    const violations = [];
    const logWarn = entry => warnings.push(entry);
    const logViolation = entry => violations.push(entry);

    const fixedNightEmpSet = new Set();
    for (const rule of this.fixedRules) {
      if (!String(rule.shiftCode || "").includes("常夜")) continue;
      const rangeDates = expandDateRange(rule.dateFrom, rule.dateTo);
      const hasWindowOverlap = rangeDates.some(date => date >= windowStartDate && date <= windowDays[windowDays.length - 1].date);
      if (hasWindowOverlap) fixedNightEmpSet.add(rule.empId);
    }

    const demandByDate = new Map();
    for (const item of this.demand) {
      const dateKey = fmtDate(item.date);
      if (!demandByDate.has(dateKey)) {
        demandByDate.set(dateKey, { early: 0, noon: 0, night: 0, total: 0 });
      }
      const entry = demandByDate.get(dateKey);
      if (isEarlyCode_(item.shiftCode)) entry.early += item.required;
      if (isNoonCode_(item.shiftCode)) entry.noon += item.required;
      if (isNightCode_(item.shiftCode)) entry.night += item.required;
      entry.total += item.required;
    }

    const defaultDemand = { early: 1, noon: 1, night: 1, total: 3 };
    const getDailyDemand = dateKey => demandByDate.get(dateKey) || defaultDemand;

    const quotaByEmp = new Map(this.people.map(p => [p.empId, { quotaSun: 5, quotaOffTotal: 10 }]));

    const dailyLeaveCounts = new Map();
    for (let idx = 0; idx < windowDays.length; idx += 1) {
      const dateKey = dateKeys[idx];
      const leaveCount = this.leave.filter(item => fmtDate(item.date) === dateKey).length;
      dailyLeaveCounts.set(dateKey, leaveCount);
    }

    const dailyOffCap = new Map();
    for (let idx = 0; idx < windowDays.length; idx += 1) {
      const dateKey = dateKeys[idx];
      const demand = getDailyDemand(dateKey);
      const needNTotal = demand.night;
      const totalNeed = demand.early + demand.noon + needNTotal;
      const maxOff = Math.max(0, this.people.length - totalNeed);
      dailyOffCap.set(dateKey, maxOff);
      if (this.people.length < totalNeed) {
        logWarn({ type: "coverage_insufficient_staff", date: dateKey, totalNeed, totalStaff: this.people.length });
      }
      const leaveCount = dailyLeaveCounts.get(dateKey) || 0;
      if (leaveCount > maxOff) {
        logWarn({ type: "off_cap_exceeded_by_leave", date: dateKey, leaveCount, maxOff });
      }
    }

    const getCellCodes = (empId, dateKey) => plan.get(empId).get(dateKey) || [];
    const getCellStatus = (empId, dateKey) => statusPlan.get(empId).get(dateKey);
    const setCell = (empId, dateKey, codes, status) => {
      plan.get(empId).set(dateKey, codes);
      if (status) statusPlan.get(empId).set(dateKey, status);
    };

    const hasOffCode = codes => codes.some(code => isOffCode(code));
    const hasWorkCode = codes => codes.some(code => isWorkCode(code));
    const isEarlyOrNoon = code => code === coverageShiftCodes.early || code === coverageShiftCodes.noon;

    const getWorkWindow = (dateKey, codes) => {
      const windows = codes
        .map(code => getShiftStartEnd(dateKey, code))
        .filter(Boolean);
      if (!windows.length) return null;
      const start = new Date(Math.min(...windows.map(item => item.start.getTime())));
      const end = new Date(Math.max(...windows.map(item => item.end.getTime())));
      const minRestHours = Math.max(...windows.map(item => item.minRestHours || 11));
      return { start, end, minRestHours };
    };

    const minRestOk = (empId, idx, candidateShiftCode) => {
      if (!isWorkCode(candidateShiftCode)) return true;
      const byDate = plan.get(empId);
      if (!byDate) return false;
      const dateKey = dateKeys[idx];
      const existingCodes = byDate.get(dateKey) || [];
      const candidateCodes = existingCodes.concat(candidateShiftCode);
      const candidateWindow = getWorkWindow(dateKey, candidateCodes);
      if (!candidateWindow) return false;
      const prevIdx = idx - 1;
      if (prevIdx >= 0) {
        const prevCodes = byDate.get(dateKeys[prevIdx]) || [];
        const prevWindow = getWorkWindow(dateKeys[prevIdx], prevCodes);
        if (prevWindow) {
          const hours = (candidateWindow.start.getTime() - prevWindow.end.getTime()) / 36e5;
          if (hours < candidateWindow.minRestHours) return false;
        }
      }
      const nextIdx = idx + 1;
      if (nextIdx < windowDays.length) {
        const nextCodes = byDate.get(dateKeys[nextIdx]) || [];
        const nextWindow = getWorkWindow(dateKeys[nextIdx], nextCodes);
        if (nextWindow) {
          const hours = (nextWindow.start.getTime() - candidateWindow.end.getTime()) / 36e5;
          if (hours < candidateWindow.minRestHours) return false;
        }
      }
      return true;
    };

    const canAssign = (empId, idx, shiftCode) => {
      const dateKey = dateKeys[idx];
      const codes = getCellCodes(empId, dateKey);
      const status = getCellStatus(empId, dateKey);
      if (status !== CELL_STATUS.EMPTY) return false;
      if (hasOffCode(codes)) return false;
      if (!shiftByCode.has(shiftCode)) return false;
      if (codes.includes(shiftCode)) return false;
      if (shiftCode === coverageShiftCodes.longNight && !fixedNightEmpSet.has(empId)) return false;
      if (codes.length > 0 && !codes.every(isEarlyOrNoon)) return false;
      if (codes.length > 0 && !isEarlyOrNoon(shiftCode)) return false;
      if (!minRestOk(empId, idx, shiftCode)) return false;
      return true;
    };

    const canAssignOff = (empId, idx) => {
      const dateKey = dateKeys[idx];
      const status = getCellStatus(empId, dateKey);
      if (status !== CELL_STATUS.EMPTY) return false;
      const codes = getCellCodes(empId, dateKey);
      if (hasWorkCode(codes)) return false;
      if (hasOffCode(codes)) return false;
      return true;
    };

    const countOffInRange = (empId, startIdx, endIdx) => {
      let count = 0;
      for (let idx = startIdx; idx <= endIdx; idx += 1) {
        if (hasOffCode(getCellCodes(empId, dateKeys[idx]))) count += 1;
      }
      return count;
    };

    // Phase 0: seed existing schedule for the entire 35-day window
    const seedMatrix = options.seedMatrix instanceof Map ? options.seedMatrix : null;
    if (seedMatrix) {
      for (const [empId, byDateSeed] of seedMatrix.entries()) {
        const byDate = plan.get(empId);
        const statusByDate = statusPlan.get(empId);
        if (!byDate) continue;
        for (const [dateKey, rawCode] of byDateSeed.entries()) {
          if (!byDate.has(dateKey)) continue;
          if (byDate.get(dateKey).length > 0) continue;
          const code = normalizeSeedCode(dateKey, rawCode);
          if (!code) continue;
          byDate.set(dateKey, [code]);
          statusByDate.set(dateKey, CELL_STATUS.LOCKED_SEED);
        }
      }
    }

    const dailyOffCount = new Map(windowDays.map(item => [item.dateKey, 0]));

    const updateOffCount = dateKey => {
      let offCount = 0;
      for (const person of this.people) {
        const codes = getCellCodes(person.empId, dateKey);
        if (hasOffCode(codes)) offCount += 1;
      }
      dailyOffCount.set(dateKey, offCount);
    };

    const assignOff = (empId, idx, code, status) => {
      if (!canAssignOff(empId, idx)) return false;
      const dateKey = dateKeys[idx];
      setCell(empId, dateKey, [code], status);
      updateOffCount(dateKey);
      return true;
    };

    // Phase A: Leave (fixed off)
    for (const item of this.leave) {
      const dateKey = fmtDate(item.date);
      const byDate = plan.get(item.empId);
      if (byDate && byDate.has(dateKey)) {
        setCell(item.empId, dateKey, [item.leaveType], CELL_STATUS.LOCKED_LEAVE);
        updateOffCount(dateKey);
      }
    }

    // Phase B: seed from LAST and tally quota
    const quotaCountByEmp = new Map(this.people.map(p => [p.empId, { R: 0, r: 0 }]));
    for (const person of this.people) {
      const byDate = plan.get(person.empId);
      if (!byDate) continue;
      for (const { dateKey } of windowDays) {
        const codes = byDate.get(dateKey);
        if (codes.includes(SHIFT_TYPES.REST_SUN)) quotaCountByEmp.get(person.empId).R += 1;
        if (codes.includes(SHIFT_TYPES.REST_GENERAL)) quotaCountByEmp.get(person.empId).r += 1;
      }
    }

    const fixedShiftByEmp = new Map();
    for (const rule of this.fixedRules) {
      if (!fixedShiftByEmp.has(rule.empId)) fixedShiftByEmp.set(rule.empId, new Map());
      const byDate = fixedShiftByEmp.get(rule.empId);
      for (const dt of expandDateRange(rule.dateFrom, rule.dateTo)) {
        const dateKey = fmtDate(dt);
        if (!dayIndex.has(dateKey)) continue;
        byDate.set(dateKey, rule.shiftCode);
      }
    }

    // Phase B: apply fixed shifts (non-LN) without breaking OFF
    for (const person of this.people) {
      const byDate = plan.get(person.empId);
      const fixedByDate = fixedShiftByEmp.get(person.empId);
      if (!byDate || !fixedByDate) continue;
      for (const [dateKey, shiftCode] of fixedByDate.entries()) {
        const idx = dayIndex.get(dateKey);
        if (idx === undefined) continue;
        if (hasOffCode(byDate.get(dateKey))) continue;
        if (String(shiftCode || "").includes("常夜")) continue;
        if (byDate.get(dateKey).length > 0) continue;
        if (canAssign(person.empId, idx, shiftCode)) {
          byDate.set(dateKey, [shiftCode]);
          statusPlan.get(person.empId).set(dateKey, CELL_STATUS.LOCKED_SEED);
        } else {
          logWarn({ type: "fixed_shift_unassigned", empId: person.empId, date: dateKey, shiftCode });
        }
      }
    }

    // Phase C: daily maxOff and seed OFF count warnings
    for (let idx = 0; idx < windowDays.length; idx += 1) {
      const dateKey = dateKeys[idx];
      updateOffCount(dateKey);
      const maxOff = dailyOffCap.get(dateKey) || 0;
      const offCount = dailyOffCount.get(dateKey) || 0;
      if (offCount > maxOff) {
        logWarn({ type: "off_cap_exceeded_by_seed", date: dateKey, offCount, maxOff });
      }
    }

    const ensureWeeklyOff = () => {
      for (const person of this.people) {
        for (const bucket of weekBucketsIdx) {
          const hasOff = bucket.some(idx => hasOffCode(getCellCodes(person.empId, dateKeys[idx])));
          if (hasOff) continue;
          const sorted = bucket.slice().sort((a, b) => (dailyOffCount.get(dateKeys[a]) || 0) - (dailyOffCount.get(dateKeys[b]) || 0));
          let placed = false;
          for (const idx of sorted) {
            if (assignOff(person.empId, idx, SHIFT_TYPES.REST_GENERAL, CELL_STATUS.LOCKED_OFF)) {
              placed = true;
              break;
            }
          }
          if (!placed) {
            logWarn({ type: "weekly_off_missing", empId: person.empId, weekStart: dateKeys[bucket[0]] });
          }
        }
      }
    };

    const assignQuotaOff = (person, type, targets) => {
      const remaining = targets - quotaCountByEmp.get(person.empId)[type];
      if (remaining <= 0) return;
      const candidateIdx = type === "R"
        ? sundayIdx.slice()
        : windowDays.map((_, idx) => idx);
      candidateIdx.sort((a, b) => (dailyOffCount.get(dateKeys[a]) || 0) - (dailyOffCount.get(dateKeys[b]) || 0));
      let toAssign = remaining;
      for (const idx of candidateIdx) {
        if (toAssign <= 0) break;
        if (type === "R" && dow[idx] !== 0) continue;
        if (assignOff(person.empId, idx, type === "R" ? SHIFT_TYPES.REST_SUN : SHIFT_TYPES.REST_GENERAL, CELL_STATUS.LOCKED_OFF)) {
          quotaCountByEmp.get(person.empId)[type] += 1;
          toAssign -= 1;
          const maxOff = dailyOffCap.get(dateKeys[idx]) || 0;
          const offCount = dailyOffCount.get(dateKeys[idx]) || 0;
          if (offCount > maxOff) {
            logWarn({ type: "off_cap_exceeded", date: dateKeys[idx], offCount, maxOff });
          }
        }
      }
      if (toAssign > 0) {
        logWarn({ type: "missing_quota", empId: person.empId, quotaType: type, remaining: toAssign });
      }
    };

    // Phase D: fill OFF quotas (weekly, R, r)
    ensureWeeklyOff();
    for (const person of this.people) {
      assignQuotaOff(person, "R", 5);
    }
    for (const person of this.people) {
      assignQuotaOff(person, "r", 5);
    }

    // Phase E: assign long night LN
    for (const empId of fixedNightEmpSet) {
      for (let idx = 0; idx < windowDays.length; idx += 1) {
        const dateKey = dateKeys[idx];
        if (!canAssign(empId, idx, coverageShiftCodes.longNight)) continue;
        setCell(empId, dateKey, [coverageShiftCodes.longNight], CELL_STATUS.LOCKED_LN);
      }
    }

    const assignedCount = new Map(this.people.map(p => [p.empId, 0]));
    const nightCount = new Map(this.people.map(p => [p.empId, 0]));

    const pickCandidate = (idx, shiftCode, options = {}) => {
      const candidates = [];
      for (const person of this.people) {
        if (options.exclude && options.exclude.has(person.empId)) continue;
        if (!canAssign(person.empId, idx, shiftCode)) continue;
        candidates.push(person.empId);
      }
      if (!candidates.length) return null;
      candidates.sort((a, b) => {
        const weekIdx = Math.floor(idx / 7);
        const weekStart = weekIdx * 7;
        const weekEnd = weekStart + 6;
        const aWeekOff = countOffInRange(a, weekStart, weekEnd);
        const bWeekOff = countOffInRange(b, weekStart, weekEnd);
        if (aWeekOff === 0 && bWeekOff > 0) return -1;
        if (bWeekOff === 0 && aWeekOff > 0) return 1;
        const rangeStart = Math.max(0, idx - 6);
        const aRecentOff = countOffInRange(a, rangeStart, idx) > 0;
        const bRecentOff = countOffInRange(b, rangeStart, idx) > 0;
        if (aRecentOff && !bRecentOff) return -1;
        if (bRecentOff && !aRecentOff) return 1;
        const nightDiff = (nightCount.get(a) || 0) - (nightCount.get(b) || 0);
        if (nightDiff !== 0) return nightDiff;
        return (assignedCount.get(a) || 0) - (assignedCount.get(b) || 0);
      });
      return candidates[0] || null;
    };

    const markAssigned = (empId, idx, shiftCode) => {
      const dateKey = dateKeys[idx];
      const codes = getCellCodes(empId, dateKey);
      if (!codes.includes(shiftCode)) {
        codes.push(shiftCode);
        plan.get(empId).set(dateKey, codes);
      }
      assignedCount.set(empId, (assignedCount.get(empId) || 0) + 1);
      if (shiftCode === coverageShiftCodes.night || shiftCode === coverageShiftCodes.longNight) {
        nightCount.set(empId, (nightCount.get(empId) || 0) + 1);
      }
    };

    for (const person of this.people) {
      const byDate = plan.get(person.empId);
      if (!byDate) continue;
      for (const { dateKey } of windowDays) {
        const codes = byDate.get(dateKey);
        if (!hasWorkCode(codes)) continue;
        assignedCount.set(person.empId, (assignedCount.get(person.empId) || 0) + codes.length);
        if (codes.some(code => isNightCode_(code))) {
          nightCount.set(person.empId, (nightCount.get(person.empId) || 0) + 1);
        }
      }
    }

    const dailyNightTaken = new Map(windowDays.map(item => [item.dateKey, false]));
    for (const { dateKey } of windowDays) {
      for (const person of this.people) {
        const codes = plan.get(person.empId).get(dateKey);
        if (codes.includes(coverageShiftCodes.longNight)) {
          dailyNightTaken.set(dateKey, true);
          break;
        }
      }
    }

    // Phase F: fill night coverage N
    for (let idx = 0; idx < windowDays.length; idx += 1) {
      const dateKey = dateKeys[idx];
      const demand = getDailyDemand(dateKey);
      if (dailyNightTaken.get(dateKey)) continue;
      const needNTotal = demand.night;
      for (let count = 0; count < needNTotal; count += 1) {
        const candidate = pickCandidate(idx, coverageShiftCodes.night, { exclude: fixedNightEmpSet });
        if (candidate) {
          markAssigned(candidate, idx, coverageShiftCodes.night);
          statusPlan.get(candidate).set(dateKey, CELL_STATUS.LOCKED_N);
          dailyNightTaken.set(dateKey, true);
        } else {
          logWarn({ type: "no_feasible_night", date: dateKey, reason: "minRest/leave/offCap" });
          break;
        }
      }
    }

    // Phase G: fill early and noon coverage
    for (let idx = 0; idx < windowDays.length; idx += 1) {
      const dateKey = dateKeys[idx];
      const demand = getDailyDemand(dateKey);

      if (shiftByCode.has(coverageShiftCodes.early)) {
        let currentEarly = 0;
        for (const person of this.people) {
          const codes = plan.get(person.empId).get(dateKey);
          if (codes.includes(coverageShiftCodes.early)) currentEarly += 1;
        }
        for (let count = currentEarly; count < demand.early; count += 1) {
          const candidate = pickCandidate(idx, coverageShiftCodes.early);
          if (candidate) {
            markAssigned(candidate, idx, coverageShiftCodes.early);
          } else {
            logWarn({ type: "no_feasible_early", date: dateKey, reason: "minRest/leave/offCap" });
            break;
          }
        }
      }

      if (shiftByCode.has(coverageShiftCodes.noon)) {
        let currentNoon = 0;
        for (const person of this.people) {
          const codes = plan.get(person.empId).get(dateKey);
          if (codes.includes(coverageShiftCodes.noon)) currentNoon += 1;
        }
        for (let count = currentNoon; count < demand.noon; count += 1) {
          const candidate = pickCandidate(idx, coverageShiftCodes.noon);
          if (candidate) {
            markAssigned(candidate, idx, coverageShiftCodes.noon);
          } else {
            logWarn({ type: "no_feasible_noon", date: dateKey, reason: "minRest/leave/offCap" });
            break;
          }
        }
      }
    }

    const validatePlanDetailed = () => {
      const coverage = [];
      for (let idx = 0; idx < windowDays.length; idx += 1) {
        const dateKey = dateKeys[idx];
        const demand = getDailyDemand(dateKey);
        const needNTotal = demand.night;
        let earlyCount = 0;
        let noonCount = 0;
        let nightCount = 0;
        let longNightCount = 0;
        let offCount = 0;
        for (const person of this.people) {
          const codes = plan.get(person.empId).get(dateKey);
          if (hasOffCode(codes)) offCount += 1;
          if (codes.includes(coverageShiftCodes.early)) earlyCount += 1;
          if (codes.includes(coverageShiftCodes.noon)) noonCount += 1;
          if (codes.includes(coverageShiftCodes.night)) nightCount += 1;
          if (codes.includes(coverageShiftCodes.longNight)) longNightCount += 1;
        }
        const nightCoverage = nightCount + longNightCount;
        const availableCount = this.people.length - offCount;
        coverage.push({ date: dateKey, earlyCount, noonCount, nightCoverage, offCount });
        if (earlyCount < demand.early || noonCount < demand.noon || nightCoverage < needNTotal) {
          logViolation({
            date: dateKey,
            type: "daily_coverage",
            details: `early=${earlyCount} noon=${noonCount} nightCoverage=${nightCoverage}`,
            leaveCount: dailyLeaveCounts.get(dateKey) || 0,
            offCount,
            availableCount
          });
        }
        if (longNightCount >= 1 && nightCount > 0) {
          logViolation({ date: dateKey, type: "night_mutual_exclusion", details: `LN=${longNightCount} N=${nightCount}` });
        }
        const maxOff = dailyOffCap.get(dateKey) || 0;
        if (offCount > maxOff) {
          logWarn({ date: dateKey, type: "off_cap_exceeded", offCount, maxOff });
        }
      }

      for (const person of this.people) {
        const byDate = plan.get(person.empId);
        if (!byDate) continue;
        const quota = quotaByEmp.get(person.empId) || { quotaSun: 5, quotaOffTotal: 10 };
        const sunCount = sundayIdx.reduce((sum, idx) => sum + (byDate.get(dateKeys[idx]).includes(SHIFT_TYPES.REST_SUN) ? 1 : 0), 0);
        const offTotal = windowDays.reduce((sum, { dateKey }) => sum + (hasOffCode(byDate.get(dateKey)) ? 1 : 0), 0);
        const rCount = windowDays.reduce((sum, { dateKey }) => sum + (byDate.get(dateKey).includes(SHIFT_TYPES.REST_GENERAL) ? 1 : 0), 0);
        if (sunCount !== quota.quotaSun) {
          logViolation({ empId: person.empId, type: "quota_sun", details: `target=${quota.quotaSun} actual=${sunCount}` });
        }
        if (rCount !== 5) {
          logViolation({ empId: person.empId, type: "quota_r", details: `target=5 actual=${rCount}` });
        }
        if (offTotal < quota.quotaOffTotal) {
          logViolation({ empId: person.empId, type: "quota_off_total", details: `target=${quota.quotaOffTotal} actual=${offTotal}` });
        }

        for (const bucket of weekBucketsIdx) {
          const hasOff = bucket.some(idx => hasOffCode(byDate.get(dateKeys[idx])));
          if (!hasOff) {
            logViolation({ empId: person.empId, type: "weekly_off_missing", details: `weekStart=${dateKeys[bucket[0]]}` });
          }
        }

        for (let idx = 1; idx < windowDays.length; idx += 1) {
          const prevKey = dateKeys[idx - 1];
          const curKey = dateKeys[idx];
          const prevCodes = byDate.get(prevKey);
          const curCodes = byDate.get(curKey);
          if (hasWorkCode(prevCodes) && hasWorkCode(curCodes)) {
            const prevShift = getWorkWindow(prevKey, prevCodes);
            const curShift = getWorkWindow(curKey, curCodes);
            if (!prevShift || !curShift) {
              logViolation({ empId: person.empId, type: "min_rest", details: `prevDate=${prevKey} curDate=${curKey}` });
            } else {
              const hours = (curShift.start.getTime() - prevShift.end.getTime()) / 36e5;
              if (hours < (Number(curShift.minRestHours) || 11)) {
                logViolation({ empId: person.empId, type: "min_rest", details: `prevDate=${prevKey} curDate=${curKey}` });
              }
            }
          }
        }

        if (fixedNightEmpSet.has(person.empId)) {
          for (const { dateKey } of windowDays) {
            const codes = byDate.get(dateKey);
            if (hasWorkCode(codes) && !codes.includes(coverageShiftCodes.longNight)) {
              logViolation({ empId: person.empId, type: "fixed_night", details: `date=${dateKey} code=${codes.join("+")}` });
            }
          }
        }
      }
      return { ok: violations.length === 0, warnings, coverage };
    };

    const validation = validatePlanDetailed();
    for (const violation of violations) {
      Logger.log(`[VIOLATION] ${JSON.stringify(violation)}`);
    }
    for (const warning of warnings) {
      Logger.log(`[WARN] ${JSON.stringify(warning)}`);
    }
    Logger.log(`[SUMMARY] window=${dateKeys[0]}~${dateKeys[dateKeys.length - 1]}`);
    for (const entry of validation.coverage || []) {
      Logger.log(`[SUMMARY] date=${entry.date} early=${entry.earlyCount} noon=${entry.noonCount} nightCoverage=${entry.nightCoverage} offCount=${entry.offCount}`);
    }

    this.monthPlan = { month, days, windowDays, plan };
    return this.monthPlan;
  }

  toMonthMatrix() {
    if (!this.monthPlan) throw new Error("Month plan not built");
    const dowRow = ["empId", "name", ...this.monthPlan.days.map(d => "日一二三四五六"[dow_(d.date)])];
    const dayRow = ["empId", "name", ...this.monthPlan.days.map(d => d.day)];
    const headers = [dowRow, dayRow];
    const matrix = this.people.map(person => {
      const byDate = this.monthPlan.plan.get(person.empId);
      const row = [person.empId, person.name];
      for (const { dateKey } of this.monthPlan.days) {
        const codes = byDate ? byDate.get(dateKey) || [] : [];
        row.push(codes.length ? codes.join("+") : "");
      }
      return row;
    });
    return { headers, matrix };
  }

  toWindowMatrix() {
    if (!this.monthPlan) throw new Error("Month plan not built");
    const dowRow = ["empId", "name", ...this.monthPlan.windowDays.map(d => "日一二三四五六"[dow_(d.date)])];
    const dayRow = ["empId", "name", ...this.monthPlan.windowDays.map(d => d.date.getDate())];
    const headers = [dowRow, dayRow];
    const matrix = this.people.map(person => {
      const byDate = this.monthPlan.plan.get(person.empId);
      const row = [person.empId, person.name];
      for (const { dateKey } of this.monthPlan.windowDays) {
        const codes = byDate ? byDate.get(dateKey) || [] : [];
        row.push(codes.length ? codes.join("+") : "");
      }
      return row;
    });
    return { headers, matrix };
  }
}

// utils
function fmtDate(d) {
  return Utilities.formatDate(new Date(d), "Asia/Taipei", "yyyy-MM-dd");
}
function expandDateRange(from, to) {
  if (!from && !to) return [];
  const start = new Date(from);
  const end = to ? new Date(to) : new Date(from);
  const out = [];
  for (let dt = new Date(start); dt <= end; dt.setDate(dt.getDate()+1)) out.push(new Date(dt));
  return out;
}
function dow_(dateObj) {
  return new Date(dateObj).getDay();
}
function weekIndex_(dateObj, monthStart) {
  const d0 = new Date(monthStart);
  const d = new Date(dateObj);
  let idx = 0;
  for (let x = new Date(d0); x <= d; x.setDate(x.getDate() + 1)) {
    if (x.getTime() === d0.getTime()) continue;
    if (x.getDay() === 0) idx++;
  }
  return idx;
}
