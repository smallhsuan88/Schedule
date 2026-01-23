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
    const dayIndex = new Map(windowDays.map((item, idx) => [item.dateKey, idx]));

    const shiftByCode = new Map(Object.values(this.shiftDefs).map(def => [def.shiftCode, def]));

    const SHIFT_TYPES = Object.freeze({
      EARLY: "E",
      NOON: "M",
      NIGHT: "N",
      LONG_NIGHT: "LN",
      OFF: "OFF",
      REST_WEEKLY: "R",
      REST_GENERAL: "r"
    });
    const softPreference = {
      changePenalty: Number(options.changePenalty ?? 3),
      sameShiftBonus: Number(options.sameShiftBonus ?? 2)
    };

    const CELL_STATUS = Object.freeze({
      EMPTY: "EMPTY",
      ASSIGNED: "ASSIGNED",
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

    const allowedShiftCodes = new Set([
      SHIFT_TYPES.REST_WEEKLY,
      SHIFT_TYPES.REST_GENERAL,
      ...shiftByCode.keys()
    ]);
    for (const leaveType of leaveTypes) {
      if (leaveType) allowedShiftCodes.add(String(leaveType).normalize("NFKC").trim());
    }
    if (shiftByCode.has("特") || leaveTypes.has("特")) allowedShiftCodes.add("特");
    if (shiftByCode.has("補") || leaveTypes.has("補")) allowedShiftCodes.add("補");
    if (shiftByCode.has("師") || leaveTypes.has("師")) allowedShiftCodes.add("師");

    const normalizeShiftCode = raw => {
      if (raw === null || raw === undefined) return "";
      const rawStr = String(raw).normalize("NFKC").replace(/[\u200B-\u200D\uFEFF]/g, "").trim();
      if (!rawStr) return "";
      if (rawStr.includes("+")) return "INVALID_MULTI_SHIFT";
      const tokens = rawStr.split(/\s+/).filter(Boolean);
      if (tokens.length > 1) return "INVALID_MULTI_SHIFT";
      const code = tokens[0];
      if (!allowedShiftCodes.has(code)) return "INVALID_SHIFT";
      return code;
    };

    const normalizeSeedCode = code => {
      if (!code) return "";
      const rawStr = String(code).normalize("NFKC").replace(/[\u200B-\u200D\uFEFF]/g, "").trim();
      if (!rawStr) return "";
      if (rawStr === "R_sun" || rawStr === "R_sat") return SHIFT_TYPES.REST_WEEKLY;
      if (rawStr === "R") return SHIFT_TYPES.REST_WEEKLY;
      return rawStr;
    };

    const isLeaveCode_ = code => {
      if (!code) return false;
      if (code === SHIFT_TYPES.REST_WEEKLY || code === SHIFT_TYPES.REST_GENERAL) return false;
      if (code === coverageShiftCodes.early || code === coverageShiftCodes.noon || code === coverageShiftCodes.night || code === coverageShiftCodes.longNight) {
        return false;
      }
      return allowedShiftCodes.has(code) && !shiftByCode.has(code);
    };
    const isOffCode = code => code === SHIFT_TYPES.REST_WEEKLY || code === SHIFT_TYPES.REST_GENERAL || isLeaveCode_(code);
    const isWorkCode = code => Boolean(code) && !isOffCode(code);
    const isNightCode_ = code => code === coverageShiftCodes.night || code === coverageShiftCodes.longNight || String(code || "").includes("常夜");
    const isEarlyCode_ = code => code === "早" || code === "早L";
    const isNoonCode_ = code => code === "午" || code === "午L";

    const plan = new Map();
    const statusPlan = new Map();
    for (const person of this.people) {
      const byDate = new Map();
      const statusByDate = new Map();
      for (const { dateKey } of windowDays) {
        byDate.set(dateKey, "");
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
    const dateByKey = new Map(windowDays.map(item => [item.dateKey, item.date]));

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

    const dailyLeaveCounts = new Map();
    for (let idx = 0; idx < windowDays.length; idx += 1) {
      const dateKey = dateKeys[idx];
      const leaveCount = this.leave.filter(item => fmtDate(item.date) === dateKey).length;
      dailyLeaveCounts.set(dateKey, leaveCount);
    }

    const getCellCode = (empId, dateKey) => plan.get(empId).get(dateKey) || "";
    const getCellStatus = (empId, dateKey) => statusPlan.get(empId).get(dateKey);
    const setCell = (empId, dateKey, code, status) => {
      plan.get(empId).set(dateKey, code);
      if (status) statusPlan.get(empId).set(dateKey, status);
    };

    const hasOffCode = code => isOffCode(code);
    const hasWorkCode = code => isWorkCode(code);
    const isEarlyOrNoon = code => code === coverageShiftCodes.early || code === coverageShiftCodes.noon;

    const getWorkWindow = (dateKey, code) => {
      if (!code || !shiftByCode.has(code)) return null;
      return getShiftStartEnd(dateKey, code);
    };

    const minRestOk = (empId, idx, candidateShiftCode) => {
      if (!isWorkCode(candidateShiftCode)) return true;
      const byDate = plan.get(empId);
      if (!byDate) return false;
      const dateKey = dateKeys[idx];
      const existingCode = byDate.get(dateKey);
      if (existingCode) return false;
      const candidateWindow = getWorkWindow(dateKey, candidateShiftCode);
      if (!candidateWindow) return false;
      const prevIdx = idx - 1;
      if (prevIdx >= 0) {
        const prevCode = byDate.get(dateKeys[prevIdx]);
        const prevWindow = getWorkWindow(dateKeys[prevIdx], prevCode);
        if (prevWindow) {
          const hours = (candidateWindow.start.getTime() - prevWindow.end.getTime()) / 36e5;
          if (hours < candidateWindow.minRestHours) return false;
        }
      }
      const nextIdx = idx + 1;
      if (nextIdx < windowDays.length) {
        const nextCode = byDate.get(dateKeys[nextIdx]);
        const nextWindow = getWorkWindow(dateKeys[nextIdx], nextCode);
        if (nextWindow) {
          const hours = (nextWindow.start.getTime() - candidateWindow.end.getTime()) / 36e5;
          if (hours < candidateWindow.minRestHours) return false;
        }
      }
      return true;
    };

    const canAssign = (empId, idx, shiftCode) => {
      const dateKey = dateKeys[idx];
      const code = getCellCode(empId, dateKey);
      const status = getCellStatus(empId, dateKey);
      if (status !== CELL_STATUS.EMPTY) return false;
      if (code) return false;
      if (hasOffCode(code)) return false;
      if (!shiftByCode.has(shiftCode)) return false;
      if (shiftCode === coverageShiftCodes.longNight && !fixedNightEmpSet.has(empId)) return false;
      if (shiftCode === coverageShiftCodes.night && (dailyLongNightCount.get(dateKey) || 0) >= 1) return false;
      if (!minRestOk(empId, idx, shiftCode)) return false;
      if (!hasSevenDayOffCoverage(empId, idx)) return false;
      return true;
    };

    const canAssignOff = (empId, idx) => {
      const dateKey = dateKeys[idx];
      const status = getCellStatus(empId, dateKey);
      if (status !== CELL_STATUS.EMPTY) return false;
      const code = getCellCode(empId, dateKey);
      if (hasWorkCode(code)) return false;
      if (hasOffCode(code)) return false;
      return true;
    };

    const countOffInRange = (empId, startIdx, endIdx) => {
      let count = 0;
      for (let idx = startIdx; idx <= endIdx; idx += 1) {
        if (hasOffCode(getCellCode(empId, dateKeys[idx]))) count += 1;
      }
      return count;
    };

    const getShiftChangeScore = (empId, idx, shiftCode) => {
      if (!hasWorkCode(shiftCode)) return 0;
      const prevIdx = idx - 1;
      if (prevIdx < 0) return 0;
      const prevCode = getCellCode(empId, dateKeys[prevIdx]);
      if (!hasWorkCode(prevCode)) return 0;
      if (prevCode === shiftCode) return -softPreference.sameShiftBonus;
      return softPreference.changePenalty;
    };

    const hasSevenDayOffCoverage = (empId, idx) => {
      const start = Math.max(0, idx - 6);
      const end = Math.min(idx, windowDays.length - 7);
      for (let startIdx = start; startIdx <= end; startIdx += 1) {
        const endIdx = startIdx + 6;
        if (countOffInRange(empId, startIdx, endIdx) === 0) {
          return false;
        }
      }
      return true;
    };

    const dailyOffCount = new Map(windowDays.map(item => [item.dateKey, 0]));

    const updateOffCount = dateKey => {
      let offCount = 0;
      for (const person of this.people) {
        const code = getCellCode(person.empId, dateKey);
        if (hasOffCode(code)) offCount += 1;
      }
      dailyOffCount.set(dateKey, offCount);
    };

    const dailyLongNightCount = new Map(windowDays.map(item => [item.dateKey, 0]));
    const getMaxOff = dateKey => {
      const demand = getDailyDemand(dateKey);
      const longNightCount = dailyLongNightCount.get(dateKey) || 0;
      const needNight = Math.max(0, demand.night - longNightCount);
      const totalNeed = demand.early + demand.noon + needNight;
      return Math.max(0, this.people.length - totalNeed);
    };
    const getOffRemaining = dateKey => {
      const maxOff = getMaxOff(dateKey);
      const offCount = dailyOffCount.get(dateKey) || 0;
      return maxOff - offCount;
    };
    const isSundayKey = dateKey => {
      const dt = dateByKey.get(dateKey);
      if (!dt) return false;
      return dt.getDay() === 0;
    };

    const tryAssign = (empId, dateKey, code, status, reason) => {
      const normalized = normalizeShiftCode(code);
      if (normalized === "INVALID_MULTI_SHIFT" || normalized === "INVALID_SHIFT") {
        logViolation({ type: "invalid_shift_code", empId, date: dateKey, code, reason });
        return false;
      }
      if (!normalized) return false;
      const existing = getCellCode(empId, dateKey);
      const existingStatus = getCellStatus(empId, dateKey);
      if (existing || existingStatus !== CELL_STATUS.EMPTY) {
        logWarn({ type: "cell_locked", empId, date: dateKey, code: normalized, reason });
        return false;
      }
      if (normalized === coverageShiftCodes.night && (dailyLongNightCount.get(dateKey) || 0) >= 1) {
        logViolation({ type: "night_mutex", empId, date: dateKey, code: normalized, reason });
        return false;
      }
      setCell(empId, dateKey, normalized, status || CELL_STATUS.ASSIGNED);
      if (normalized === coverageShiftCodes.longNight) {
        dailyLongNightCount.set(dateKey, (dailyLongNightCount.get(dateKey) || 0) + 1);
      }
      if (hasOffCode(normalized)) {
        updateOffCount(dateKey);
        const maxOff = getMaxOff(dateKey);
        const offCount = dailyOffCount.get(dateKey) || 0;
        if (offCount > maxOff) {
          logWarn({ type: "off_cap_exceeded", date: dateKey, offCount, maxOff });
        }
      }
      return true;
    };

    const assignOff = (empId, idx, code, status, reason) => {
      if (!canAssignOff(empId, idx)) return false;
      const dateKey = dateKeys[idx];
      return tryAssign(empId, dateKey, code, status, reason);
    };
    const clearCell = (empId, dateKey) => {
      plan.get(empId).set(dateKey, "");
      statusPlan.get(empId).set(dateKey, CELL_STATUS.EMPTY);
      updateOffCount(dateKey);
    };

    const applySeedMatrix = (matrix, source) => {
      if (!(matrix instanceof Map)) return;
      for (const [empId, byDateSeed] of matrix.entries()) {
        const byDate = plan.get(empId);
        if (!byDate) continue;
        for (const [dateKey, rawCode] of byDateSeed.entries()) {
          if (!byDate.has(dateKey)) continue;
          const normalized = normalizeSeedCode(rawCode);
          if (!normalized) continue;
          const result = normalizeShiftCode(normalized);
          if (result === "INVALID_MULTI_SHIFT" || result === "INVALID_SHIFT") {
            logViolation({ type: "invalid_shift_code", empId, date: dateKey, code: rawCode, source });
            continue;
          }
          tryAssign(empId, dateKey, normalized, CELL_STATUS.LOCKED_SEED, source);
        }
      }
    };

    // Phase 0: seed existing schedule for the entire 35-day window
    applySeedMatrix(options.seedMatrix, "seedMatrix");
    applySeedMatrix(options.existingMatrix, "existingMatrix");

    // Phase A: Leave (fixed off)
    for (const item of this.leave) {
      const dateKey = fmtDate(item.date);
      const byDate = plan.get(item.empId);
      if (!byDate || !byDate.has(dateKey)) continue;
      const normalized = normalizeShiftCode(item.leaveType);
      if (normalized === "INVALID_MULTI_SHIFT" || normalized === "INVALID_SHIFT") {
        logViolation({ type: "invalid_shift_code", empId: item.empId, date: dateKey, code: item.leaveType, source: "leave" });
        continue;
      }
      tryAssign(item.empId, dateKey, normalized, CELL_STATUS.LOCKED_LEAVE, "leave");
    }

    // Phase B: seed from LAST and tally quota
    const quotaCountByEmp = new Map(this.people.map(p => [p.empId, { R: 0, R_sun: 0, r: 0 }]));
    for (const person of this.people) {
      const byDate = plan.get(person.empId);
      if (!byDate) continue;
      for (const { dateKey } of windowDays) {
        const code = byDate.get(dateKey);
        if (code === SHIFT_TYPES.REST_WEEKLY) {
          quotaCountByEmp.get(person.empId).R += 1;
          if (isSundayKey(dateKey)) quotaCountByEmp.get(person.empId).R_sun += 1;
        }
        if (code === SHIFT_TYPES.REST_GENERAL) quotaCountByEmp.get(person.empId).r += 1;
      }
    }
    const quotaRemainingByEmp = new Map();
    for (const person of this.people) {
      const used = quotaCountByEmp.get(person.empId) || { R: 0, R_sun: 0, r: 0 };
      const remaining = {
        R: Math.max(0, 5 - used.R_sun),
        r: Math.max(0, 5 - used.r)
      };
      quotaRemainingByEmp.set(person.empId, remaining);
      if (used.R > 5 || used.r > 5 || used.R_sun > 5) {
        logViolation({
          type: "quota_exceeded_seeded",
          empId: person.empId,
          details: `R=${used.R} R_sun=${used.R_sun} r=${used.r}`
        });
      }
      if (used.R > used.R_sun) {
        logViolation({
          type: "rsun_non_sunday",
          empId: person.empId,
          details: `totalR=${used.R} sundayR=${used.R_sun}`
        });
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
        if (byDate.get(dateKey)) continue;
        if (canAssign(person.empId, idx, shiftCode)) {
          if (!tryAssign(person.empId, dateKey, shiftCode, CELL_STATUS.LOCKED_SEED, "fixed_shift")) {
            logWarn({ type: "fixed_shift_unassigned", empId: person.empId, date: dateKey, shiftCode });
          }
        } else {
          logWarn({ type: "fixed_shift_unassigned", empId: person.empId, date: dateKey, shiftCode });
        }
      }
    }

    // Phase C: daily maxOff and seed OFF count warnings
    for (let idx = 0; idx < windowDays.length; idx += 1) {
      const dateKey = dateKeys[idx];
      updateOffCount(dateKey);
      const demand = getDailyDemand(dateKey);
      const totalNeed = demand.early + demand.noon + demand.night;
      if (this.people.length < totalNeed) {
        logWarn({ type: "coverage_insufficient_staff", date: dateKey, totalNeed, totalStaff: this.people.length });
      }
      const maxOff = getMaxOff(dateKey);
      const offCount = dailyOffCount.get(dateKey) || 0;
      if (offCount > maxOff) {
        logWarn({ type: "off_cap_exceeded_by_seed", date: dateKey, offCount, maxOff });
      }
      const leaveCount = dailyLeaveCounts.get(dateKey) || 0;
      if (leaveCount > maxOff) {
        logWarn({ type: "off_cap_exceeded_by_leave", date: dateKey, leaveCount, maxOff });
      }
    }

    const getNearestOffDistance = (empId, idx) => {
      let left = idx - 1;
      while (left >= 0 && !hasOffCode(getCellCode(empId, dateKeys[left]))) {
        left -= 1;
      }
      let right = idx + 1;
      while (right < windowDays.length && !hasOffCode(getCellCode(empId, dateKeys[right]))) {
        right += 1;
      }
      const leftDist = left >= 0 ? idx - left : windowDays.length;
      const rightDist = right < windowDays.length ? right - idx : windowDays.length;
      return Math.min(leftDist, rightDist);
    };
    const canAssignOffWithCap = (empId, idx) => {
      if (!canAssignOff(empId, idx)) return false;
      const dateKey = dateKeys[idx];
      return getOffRemaining(dateKey) > 0;
    };
    const getRsunCandidates = empId => {
      const candidates = [];
      for (let idx = 0; idx < windowDays.length; idx += 1) {
        const dateKey = dateKeys[idx];
        if (!isSundayKey(dateKey)) continue;
        if (!canAssignOffWithCap(empId, idx)) continue;
        candidates.push(idx);
      }
      candidates.sort((a, b) => {
        const dateKeyA = dateKeys[a];
        const dateKeyB = dateKeys[b];
        const offRemainingDiff = getOffRemaining(dateKeyB) - getOffRemaining(dateKeyA);
        if (offRemainingDiff !== 0) return offRemainingDiff;
        const distanceDiff = getNearestOffDistance(empId, b) - getNearestOffDistance(empId, a);
        if (distanceDiff !== 0) return distanceDiff;
        return b - a;
      });
      return candidates;
    };
    const getRCandidates = empId => {
      const candidates = [];
      for (let idx = 0; idx < windowDays.length; idx += 1) {
        const dateKey = dateKeys[idx];
        if (!canAssignOffWithCap(empId, idx)) continue;
        candidates.push(idx);
      }
      candidates.sort((a, b) => {
        const dateKeyA = dateKeys[a];
        const dateKeyB = dateKeys[b];
        const offRemainingDiff = getOffRemaining(dateKeyB) - getOffRemaining(dateKeyA);
        if (offRemainingDiff !== 0) return offRemainingDiff;
        const distanceDiff = getNearestOffDistance(empId, b) - getNearestOffDistance(empId, a);
        if (distanceDiff !== 0) return distanceDiff;
        return b - a;
      });
      return candidates;
    };
    const canRemoveOff = (empId, idx) => {
      const dateKey = dateKeys[idx];
      const status = getCellStatus(empId, dateKey);
      if (status === CELL_STATUS.LOCKED_LEAVE || status === CELL_STATUS.LOCKED_SEED) return false;
      if (!hasOffCode(getCellCode(empId, dateKey))) return false;
      for (let startIdx = 0; startIdx <= windowDays.length - 7; startIdx += 1) {
        const endIdx = startIdx + 6;
        let count = countOffInRange(empId, startIdx, endIdx);
        if (idx >= startIdx && idx <= endIdx) count -= 1;
        if (count <= 0) {
          return false;
        }
      }
      return true;
    };
    const fillRsunQuota = empId => {
      let remaining = (quotaRemainingByEmp.get(empId) || {}).R || 0;
      while (remaining > 0) {
        const candidates = getRsunCandidates(empId);
        if (!candidates.length) {
          logWarn({ type: "missing_quota", empId, quotaType: "R_sun", remaining });
          break;
        }
        const idx = candidates[0];
        if (assignOff(empId, idx, SHIFT_TYPES.REST_WEEKLY, CELL_STATUS.ASSIGNED, "rsun_fill")) {
          quotaCountByEmp.get(empId).R += 1;
          quotaCountByEmp.get(empId).R_sun += 1;
          quotaRemainingByEmp.get(empId).R -= 1;
          remaining -= 1;
        } else {
          logWarn({ type: "missing_quota", empId, quotaType: "R_sun", remaining });
          break;
        }
      }
    };
    const fillRQuota = () => {
      let progress = true;
      while (progress) {
        progress = false;
        for (const person of this.people) {
          const remaining = (quotaRemainingByEmp.get(person.empId) || {}).r || 0;
          if (remaining <= 0) continue;
          const candidates = getRCandidates(person.empId);
          if (!candidates.length) continue;
          const idx = candidates[0];
          if (assignOff(person.empId, idx, SHIFT_TYPES.REST_GENERAL, CELL_STATUS.ASSIGNED, "r_fill")) {
            quotaCountByEmp.get(person.empId).r += 1;
            quotaRemainingByEmp.get(person.empId).r -= 1;
            progress = true;
          }
        }
      }
      for (const person of this.people) {
        const remaining = (quotaRemainingByEmp.get(person.empId) || {}).r || 0;
        if (remaining > 0) {
          logWarn({ type: "missing_quota", empId: person.empId, quotaType: "r", remaining });
        }
      }
    };
    const repairOverQuota = () => {
      for (const person of this.people) {
        const empId = person.empId;
        let totalR = quotaCountByEmp.get(empId).R || 0;
        let sundayR = quotaCountByEmp.get(empId).R_sun || 0;
        if (totalR > 5) {
          const removable = [];
          for (let idx = 0; idx < windowDays.length; idx += 1) {
            const dateKey = dateKeys[idx];
            if (getCellCode(empId, dateKey) !== SHIFT_TYPES.REST_WEEKLY) continue;
            if (!canRemoveOff(empId, idx)) continue;
            removable.push(idx);
          }
          removable.sort((a, b) => {
            const aSunday = isSundayKey(dateKeys[a]) ? 1 : 0;
            const bSunday = isSundayKey(dateKeys[b]) ? 1 : 0;
            if (aSunday !== bSunday) return aSunday - bSunday;
            return a - b;
          });
          while (totalR > 5 && removable.length) {
            const idx = removable.shift();
            const dateKey = dateKeys[idx];
            const wasSunday = isSundayKey(dateKey);
            clearCell(empId, dateKey);
            totalR -= 1;
            quotaCountByEmp.get(empId).R -= 1;
            if (wasSunday) {
              sundayR -= 1;
              quotaCountByEmp.get(empId).R_sun -= 1;
              quotaRemainingByEmp.get(empId).R = Math.max(0, 5 - quotaCountByEmp.get(empId).R_sun);
            }
          }
        }
        if (totalR > 5) {
          logViolation({ type: "quota_R_over", empId, details: `limit=5 actual=${totalR}` });
        }
      }

      for (const person of this.people) {
        const empId = person.empId;
        let totalR = quotaCountByEmp.get(empId).r || 0;
        if (totalR > 5) {
          const removable = [];
          for (let idx = 0; idx < windowDays.length; idx += 1) {
            const dateKey = dateKeys[idx];
            if (getCellCode(empId, dateKey) !== SHIFT_TYPES.REST_GENERAL) continue;
            if (!canRemoveOff(empId, idx)) continue;
            removable.push(idx);
          }
          removable.sort((a, b) => a - b);
          while (totalR > 5 && removable.length) {
            const idx = removable.shift();
            const dateKey = dateKeys[idx];
            clearCell(empId, dateKey);
            totalR -= 1;
            quotaCountByEmp.get(empId).r -= 1;
            quotaRemainingByEmp.get(empId).r = Math.max(0, 5 - quotaCountByEmp.get(empId).r);
          }
        }
        if (totalR > 5) {
          logViolation({ type: "quota_r_over", empId, details: `limit=5 actual=${totalR}` });
        }
      }
    };

    // Phase D: fill OFF quotas (R_sun then r), repair, then lock OFF
    for (const person of this.people) {
      fillRsunQuota(person.empId);
    }
    fillRQuota();
    repairOverQuota();
    fillRQuota();
    for (const person of this.people) {
      fillRsunQuota(person.empId);
    }
    for (const person of this.people) {
      const byDate = plan.get(person.empId);
      const statusByDate = statusPlan.get(person.empId);
      if (!byDate || !statusByDate) continue;
      for (const { dateKey } of windowDays) {
        if (!hasOffCode(byDate.get(dateKey))) continue;
        const status = statusByDate.get(dateKey);
        if (status === CELL_STATUS.LOCKED_LEAVE || status === CELL_STATUS.LOCKED_SEED) continue;
        statusByDate.set(dateKey, CELL_STATUS.LOCKED_OFF);
      }
    }

    // Phase E: assign long night LN
    for (const empId of fixedNightEmpSet) {
      for (let idx = 0; idx < windowDays.length; idx += 1) {
        const dateKey = dateKeys[idx];
        if (!canAssign(empId, idx, coverageShiftCodes.longNight)) continue;
        tryAssign(empId, dateKey, coverageShiftCodes.longNight, CELL_STATUS.LOCKED_LN, "long_night");
      }
    }

    const assignedCount = new Map(this.people.map(p => [p.empId, 0]));
    const nightCount = new Map(this.people.map(p => [p.empId, 0]));

    const pickCandidate = (idx, shiftCode, options = {}) => {
      const candidates = [];
      for (const person of this.people) {
        if (options.candidates && !options.candidates.has(person.empId)) continue;
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
        const aShiftChange = getShiftChangeScore(a, idx, shiftCode);
        const bShiftChange = getShiftChangeScore(b, idx, shiftCode);
        if (aShiftChange !== bShiftChange) return aShiftChange - bShiftChange;
        const nightDiff = (nightCount.get(a) || 0) - (nightCount.get(b) || 0);
        if (nightDiff !== 0) return nightDiff;
        return (assignedCount.get(a) || 0) - (assignedCount.get(b) || 0);
      });
      return candidates[0] || null;
    };

    const markAssigned = (empId, idx, shiftCode, status, reason) => {
      const dateKey = dateKeys[idx];
      if (!tryAssign(empId, dateKey, shiftCode, status || CELL_STATUS.ASSIGNED, reason)) return false;
      assignedCount.set(empId, (assignedCount.get(empId) || 0) + 1);
      if (shiftCode === coverageShiftCodes.night || shiftCode === coverageShiftCodes.longNight) {
        nightCount.set(empId, (nightCount.get(empId) || 0) + 1);
      }
      return true;
    };

    for (const person of this.people) {
      const byDate = plan.get(person.empId);
      if (!byDate) continue;
      for (const { dateKey } of windowDays) {
        const code = byDate.get(dateKey);
        if (!hasWorkCode(code)) continue;
        assignedCount.set(person.empId, (assignedCount.get(person.empId) || 0) + 1);
        if (isNightCode_(code)) {
          nightCount.set(person.empId, (nightCount.get(person.empId) || 0) + 1);
        }
      }
    }

    const dailyNightTaken = new Map(windowDays.map(item => [item.dateKey, false]));
    for (const { dateKey } of windowDays) {
      if ((dailyLongNightCount.get(dateKey) || 0) >= 1) {
        dailyNightTaken.set(dateKey, true);
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
          if (markAssigned(candidate, idx, coverageShiftCodes.night, CELL_STATUS.LOCKED_N, "night_fill")) {
            dailyNightTaken.set(dateKey, true);
          }
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
      const availableForDay = new Set();
      for (const person of this.people) {
        if (canAssign(person.empId, idx, coverageShiftCodes.early) || canAssign(person.empId, idx, coverageShiftCodes.noon)) {
          availableForDay.add(person.empId);
        }
      }

      if (shiftByCode.has(coverageShiftCodes.early)) {
        let currentEarly = 0;
        for (const person of this.people) {
          const code = plan.get(person.empId).get(dateKey);
          if (code === coverageShiftCodes.early) currentEarly += 1;
        }
        for (let count = currentEarly; count < demand.early; count += 1) {
          const candidate = pickCandidate(idx, coverageShiftCodes.early, { candidates: availableForDay });
          if (candidate) {
            if (markAssigned(candidate, idx, coverageShiftCodes.early, CELL_STATUS.ASSIGNED, "early_fill")) {
              availableForDay.delete(candidate);
            }
          } else {
            logWarn({ type: "no_feasible_early", date: dateKey, reason: "minRest/leave/offCap" });
            break;
          }
        }
      }

      if (shiftByCode.has(coverageShiftCodes.noon)) {
        let currentNoon = 0;
        for (const person of this.people) {
          const code = plan.get(person.empId).get(dateKey);
          if (code === coverageShiftCodes.noon) currentNoon += 1;
        }
        for (let count = currentNoon; count < demand.noon; count += 1) {
          const candidate = pickCandidate(idx, coverageShiftCodes.noon, { candidates: availableForDay });
          if (candidate) {
            if (markAssigned(candidate, idx, coverageShiftCodes.noon, CELL_STATUS.ASSIGNED, "noon_fill")) {
              availableForDay.delete(candidate);
            }
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
          const code = plan.get(person.empId).get(dateKey);
          if (hasOffCode(code)) offCount += 1;
          if (code === coverageShiftCodes.early) earlyCount += 1;
          if (code === coverageShiftCodes.noon) noonCount += 1;
          if (code === coverageShiftCodes.night) nightCount += 1;
          if (code === coverageShiftCodes.longNight) longNightCount += 1;
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
        const maxOff = getMaxOff(dateKey);
        if (offCount > maxOff) {
          logWarn({ date: dateKey, type: "off_cap_exceeded", offCount, maxOff });
        }
      }

      for (const person of this.people) {
        const byDate = plan.get(person.empId);
        if (!byDate) continue;
        const weeklyRCount = windowDays.reduce((sum, { dateKey }) => sum + (byDate.get(dateKey) === SHIFT_TYPES.REST_WEEKLY ? 1 : 0), 0);
        const sundayRCount = windowDays.reduce((sum, { dateKey }) => {
          if (byDate.get(dateKey) === SHIFT_TYPES.REST_WEEKLY && isSundayKey(dateKey)) return sum + 1;
          return sum;
        }, 0);
        const rCount = windowDays.reduce((sum, { dateKey }) => sum + (byDate.get(dateKey) === SHIFT_TYPES.REST_GENERAL ? 1 : 0), 0);
        if (weeklyRCount > 5) {
          logViolation({ empId: person.empId, type: "quota_R", details: `limit=5 actual=${weeklyRCount}` });
        }
        if (rCount > 5) {
          logViolation({ empId: person.empId, type: "quota_r", details: `limit=5 actual=${rCount}` });
        }
        if (sundayRCount < 5) {
          logWarn({ empId: person.empId, type: "quota_R_missing", details: `limit=5 actual=${sundayRCount}` });
        }
        if (rCount < 5) {
          logWarn({ empId: person.empId, type: "quota_r_missing", details: `limit=5 actual=${rCount}` });
        }

        for (const bucket of weekBucketsIdx) {
          const countR = bucket.reduce((sum, idx) => sum + (byDate.get(dateKeys[idx]) === SHIFT_TYPES.REST_WEEKLY ? 1 : 0), 0);
          if (countR !== 1) {
            if (countR > 1) {
              logViolation({ empId: person.empId, type: "weekly_R", details: `weekStart=${dateKeys[bucket[0]]} count=${countR}` });
            } else {
              logWarn({ empId: person.empId, type: "weekly_R_missing", details: `weekStart=${dateKeys[bucket[0]]} count=${countR}` });
            }
          }
        }
        for (const { dateKey } of windowDays) {
          const code = byDate.get(dateKey);
          if (code === SHIFT_TYPES.REST_WEEKLY && !isSundayKey(dateKey)) {
            logViolation({ empId: person.empId, type: "rsun_non_sunday", details: `date=${dateKey}` });
          }
        }

        for (let startIdx = 0; startIdx <= windowDays.length - 7; startIdx += 1) {
          const endIdx = startIdx + 6;
          if (countOffInRange(person.empId, startIdx, endIdx) === 0) {
            logViolation({ empId: person.empId, type: "seven_day_off_missing", details: `start=${dateKeys[startIdx]} end=${dateKeys[endIdx]}` });
          }
        }

        for (let idx = 1; idx < windowDays.length; idx += 1) {
          const prevKey = dateKeys[idx - 1];
          const curKey = dateKeys[idx];
          const prevCode = byDate.get(prevKey);
          const curCode = byDate.get(curKey);
          if (hasWorkCode(prevCode) && hasWorkCode(curCode)) {
            const prevShift = getWorkWindow(prevKey, prevCode);
            const curShift = getWorkWindow(curKey, curCode);
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
            const code = byDate.get(dateKey);
            if (hasWorkCode(code) && code !== coverageShiftCodes.longNight) {
              logViolation({ empId: person.empId, type: "fixed_night", details: `date=${dateKey} code=${code}` });
            }
          }
        }
      }
      return { ok: violations.length === 0, warnings, coverage };
    };

    const validateFinal = () => {
      let ok = true;
      for (const person of this.people) {
        const byDate = plan.get(person.empId);
        if (!byDate) continue;
        let weeklyRCount = 0;
        let sundayRCount = 0;
        let rCount = 0;
        for (const { dateKey } of windowDays) {
          const code = byDate.get(dateKey);
          if (!code) continue;
          if (typeof code === "string") {
            const normalized = String(code).normalize("NFKC").replace(/[\u200B-\u200D\uFEFF]/g, "").trim();
            if (normalized.includes("+") || normalized.split(/\s+/).filter(Boolean).length > 1) {
              logViolation({ type: "daily_single_assignment", empId: person.empId, date: dateKey, code });
              ok = false;
            }
          }
          const normalized = normalizeShiftCode(code);
          if (normalized === "INVALID_MULTI_SHIFT" || normalized === "INVALID_SHIFT") {
            logViolation({ type: "invalid_shift_code", empId: person.empId, date: dateKey, code });
            ok = false;
          }
          if (code === SHIFT_TYPES.REST_WEEKLY) weeklyRCount += 1;
          if (code === SHIFT_TYPES.REST_WEEKLY && isSundayKey(dateKey)) sundayRCount += 1;
          if (code === SHIFT_TYPES.REST_GENERAL) rCount += 1;
          if (code === SHIFT_TYPES.REST_WEEKLY && !isSundayKey(dateKey)) {
            logViolation({ empId: person.empId, type: "rsun_non_sunday", details: `date=${dateKey}` });
            ok = false;
          }
        }
        if (weeklyRCount > 5) {
          logViolation({ empId: person.empId, type: "quota_R", details: `limit=5 actual=${weeklyRCount}` });
          ok = false;
        }
        if (rCount > 5) {
          logViolation({ empId: person.empId, type: "quota_r", details: `limit=5 actual=${rCount}` });
          ok = false;
        }
        if (sundayRCount < 5) {
          logWarn({ empId: person.empId, type: "quota_R_missing", details: `limit=5 actual=${sundayRCount}` });
        }
        if (rCount < 5) {
          logWarn({ empId: person.empId, type: "quota_r_missing", details: `limit=5 actual=${rCount}` });
        }
        for (const bucket of weekBucketsIdx) {
          const countR = bucket.reduce((sum, idx) => sum + (byDate.get(dateKeys[idx]) === SHIFT_TYPES.REST_WEEKLY ? 1 : 0), 0);
          if (countR !== 1) {
            if (countR > 1) {
              logViolation({ empId: person.empId, type: "weekly_R", details: `weekStart=${dateKeys[bucket[0]]} count=${countR}` });
              ok = false;
            } else {
              logWarn({ empId: person.empId, type: "weekly_R_missing", details: `weekStart=${dateKeys[bucket[0]]} count=${countR}` });
            }
          }
        }
        for (let startIdx = 0; startIdx <= windowDays.length - 7; startIdx += 1) {
          const endIdx = startIdx + 6;
          if (countOffInRange(person.empId, startIdx, endIdx) === 0) {
            logViolation({ empId: person.empId, type: "seven_day_off_missing", details: `start=${dateKeys[startIdx]} end=${dateKeys[endIdx]}` });
            ok = false;
          }
        }
      }

      for (const { dateKey } of windowDays) {
        let longNightCount = 0;
        let nightCount = 0;
        for (const person of this.people) {
          const code = plan.get(person.empId).get(dateKey);
          if (code === coverageShiftCodes.longNight) longNightCount += 1;
          if (code === coverageShiftCodes.night) nightCount += 1;
        }
        if (longNightCount >= 1 && nightCount > 0) {
          logViolation({ date: dateKey, type: "night_mutual_exclusion", details: `LN=${longNightCount} N=${nightCount}` });
          ok = false;
        }
      }
      return ok;
    };

    const validation = validatePlanDetailed();
    const finalOk = validateFinal();
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
    if (!finalOk) {
      Logger.log("[SUMMARY] final_validation=FAILED");
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
        const code = byDate ? byDate.get(dateKey) || "" : "";
        row.push(code || "");
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
        const code = byDate ? byDate.get(dateKey) || "" : "";
        row.push(code || "");
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
