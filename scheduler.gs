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
      if (code === "R_sun" || code === "R_sat" || code === "r") return code;
      if (code === "R") {
        const idx = dayIndex.get(dateKey);
        if (idx !== undefined && dow[idx] === 0) return "R_sun";
        if (idx !== undefined && dow[idx] === 6) return "R_sat";
        return "r";
      }
      return code;
    };

    const isLeaveCode_ = code => {
      if (!code) return false;
      if (code === "R_sun" || code === "R_sat" || code === "r" || code === "R") return false;
      if (code === coverageShiftCodes.early || code === coverageShiftCodes.noon || code === coverageShiftCodes.night || code === coverageShiftCodes.longNight) {
        return false;
      }
      return leaveTypes.has(code) || !shiftByCode.has(code);
    };
    const isOffCode = code => code === "R_sun" || code === "R_sat" || code === "r" || isLeaveCode_(code);
    const isWorkCode = code => Boolean(code) && !isOffCode(code);
    const isNightCode_ = code => code === "夜" || code === "夜L" || String(code || "").includes("常夜");
    const isEarlyCode_ = code => code === "早" || code === "早L";
    const isNoonCode_ = code => code === "午" || code === "午L";

    const plan = new Map();
    const dateByKey = new Map(windowDays.map(item => [item.dateKey, item.date]));
    for (const person of this.people) {
      const byDate = new Map();
      for (const { dateKey } of windowDays) {
        byDate.set(dateKey, "");
      }
      plan.set(person.empId, byDate);
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
    const saturdayIdx = [];
    for (let idx = 0; idx < windowDays.length; idx += 1) {
      const dayOfWeek = dow[idx];
      if (dayOfWeek === 0) sundayIdx.push(idx);
      if (dayOfWeek === 6) saturdayIdx.push(idx);
    }
    const windowSundayCount = sundayIdx.length;
    const windowSaturdayCount = saturdayIdx.length;

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

    const getQuotaForPerson = person => {
      const quotaSun = Number(person.quotaSun);
      const quotaSat = Number(person.quotaSat);
      const quotaTotal = Number(person.quotaOffTotal);
      const monthlyOff = Number(person.monthlyOff || person.monthOff || person.offDays);
      const computedTotal = Number.isNaN(monthlyOff)
        ? windowSundayCount + windowSaturdayCount
        : Math.round((monthlyOff / daysInMonth) * 35);
      return {
        quotaSun: Number.isNaN(quotaSun) ? windowSundayCount : quotaSun,
        quotaSat: Number.isNaN(quotaSat) ? windowSaturdayCount : quotaSat,
        quotaOffTotal: Number.isNaN(quotaTotal) ? computedTotal : quotaTotal
      };
    };

    const quotaByEmp = new Map(this.people.map(p => [p.empId, getQuotaForPerson(p)]));

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

    // Phase 0: seed existing schedule for the entire 35-day window
    const seedMatrix = options.seedMatrix instanceof Map ? options.seedMatrix : null;
    if (seedMatrix) {
      for (const [empId, byDateSeed] of seedMatrix.entries()) {
        const byDate = plan.get(empId);
        if (!byDate) continue;
        for (const [dateKey, rawCode] of byDateSeed.entries()) {
          if (!byDate.has(dateKey)) continue;
          if (byDate.get(dateKey) !== "") continue;
          const code = normalizeSeedCode(dateKey, rawCode);
          if (!code) continue;
          byDate.set(dateKey, code);
        }
      }
    }

    const lockedOff = new Set();
    const lockedShift = new Set();
    const lockKey = (empId, idx) => `${empId}#${idx}`;

    const assignOff = (empId, idx, code, options = {}) => {
      const dateKey = dateKeys[idx];
      const byDate = plan.get(empId);
      if (!byDate) return false;
      if (isLeaveCode_(byDate.get(dateKey))) return false;
      if (lockedOff.has(lockKey(empId, idx)) && !options.allowOverride) return false;
      if (lockedShift.has(lockKey(empId, idx)) && !options.allowOverride) return false;
      byDate.set(dateKey, code);
      return true;
    };

    // Stage 1-1: Leave (fixed off)
    for (const item of this.leave) {
      const dateKey = fmtDate(item.date);
      const byDate = plan.get(item.empId);
      if (byDate && byDate.has(dateKey)) {
        byDate.set(dateKey, item.leaveType);
        const idx = dayIndex.get(dateKey);
        if (idx !== undefined) lockedOff.add(lockKey(item.empId, idx));
      }
    }

    // Stage 1-2: quota R_sun / R_sat
    for (const person of this.people) {
      const byDate = plan.get(person.empId);
      if (!byDate) continue;
      const quota = quotaByEmp.get(person.empId);
      const currentSun = sundayIdx.filter(idx => byDate.get(dateKeys[idx]) === "R_sun").length;
      const currentSat = saturdayIdx.filter(idx => byDate.get(dateKeys[idx]) === "R_sat").length;
      let remainingSun = Math.max(0, (quota?.quotaSun || 0) - currentSun);
      let remainingSat = Math.max(0, (quota?.quotaSat || 0) - currentSat);

      for (const idx of sundayIdx) {
        if (remainingSun <= 0) break;
        const dateKey = dateKeys[idx];
        if (isLeaveCode_(byDate.get(dateKey))) continue;
        if (assignOff(person.empId, idx, "R_sun", { allowOverride: true })) remainingSun -= 1;
      }
      if (remainingSun > 0) {
        logWarn({ type: "missing_quota_sun", empId: person.empId, remaining: remainingSun });
      }

      for (const idx of saturdayIdx) {
        if (remainingSat <= 0) break;
        const dateKey = dateKeys[idx];
        if (isLeaveCode_(byDate.get(dateKey))) continue;
        if (assignOff(person.empId, idx, "R_sat", { allowOverride: true })) remainingSat -= 1;
      }
      if (remainingSat > 0) {
        logWarn({ type: "missing_quota_sat", empId: person.empId, remaining: remainingSat });
      }
    }

    // Stage 1-3: fill remaining OFF quota with r (non-weekend)
    const weekdayIdx = windowDays
      .map((_, idx) => idx)
      .filter(idx => dow[idx] !== 0 && dow[idx] !== 6);
    for (const person of this.people) {
      const byDate = plan.get(person.empId);
      if (!byDate) continue;
      const quota = quotaByEmp.get(person.empId);
      const currentOff = windowDays.reduce((sum, { dateKey }) => sum + (isOffCode(byDate.get(dateKey)) ? 1 : 0), 0);
      let remaining = Math.max(0, (quota?.quotaOffTotal || 0) - currentOff);
      for (const idx of weekdayIdx) {
        if (remaining <= 0) break;
        const dateKey = dateKeys[idx];
        if (isLeaveCode_(byDate.get(dateKey))) continue;
        if (assignOff(person.empId, idx, "r", { allowOverride: true })) remaining -= 1;
      }
      if (remaining > 0) {
        logWarn({ type: "missing_quota_off_total", empId: person.empId, remaining });
      }
    }

    // Stage 1-4: ensure weekly at least 1 OFF
    for (const person of this.people) {
      const byDate = plan.get(person.empId);
      if (!byDate) continue;
      for (const bucket of weekBucketsIdx) {
        const hasOff = bucket.some(idx => isOffCode(byDate.get(dateKeys[idx])));
        if (hasOff) continue;
        let placed = false;
        for (const idx of bucket) {
          if (dow[idx] === 0 || dow[idx] === 6) continue;
          if (assignOff(person.empId, idx, "r", { allowOverride: true })) {
            placed = true;
            break;
          }
        }
        if (!placed) {
          logWarn({ type: "weekly_off_missing", empId: person.empId, weekStart: dateKeys[bucket[0]] });
        }
      }
    }

    // Stage 1-5: lock OFF
    for (const person of this.people) {
      const byDate = plan.get(person.empId);
      if (!byDate) continue;
      for (let idx = 0; idx < windowDays.length; idx += 1) {
        if (isOffCode(byDate.get(dateKeys[idx]))) lockedOff.add(lockKey(person.empId, idx));
      }
    }

    // Stage 2: long night assignment
    for (const empId of fixedNightEmpSet) {
      const byDate = plan.get(empId);
      if (!byDate) continue;
      for (let idx = 0; idx < windowDays.length; idx += 1) {
        const dateKey = dateKeys[idx];
        if (isOffCode(byDate.get(dateKey))) continue;
        byDate.set(dateKey, coverageShiftCodes.longNight);
        lockedShift.add(lockKey(empId, idx));
      }
    }

    const assignedCount = new Map(this.people.map(p => [p.empId, 0]));
    const nightCount = new Map(this.people.map(p => [p.empId, 0]));

    const minRestOk = (empId, idx, candidateShiftCode) => {
      if (!isWorkCode(candidateShiftCode)) return true;
      const candidate = getShiftStartEnd(dateKeys[idx], candidateShiftCode);
      if (!candidate) return false;
      const byDate = plan.get(empId);
      if (!byDate) return false;
      const prevIdx = idx - 1;
      if (prevIdx >= 0) {
        const prevCode = byDate.get(dateKeys[prevIdx]);
        if (isWorkCode(prevCode)) {
          const prevShift = getShiftStartEnd(dateKeys[prevIdx], prevCode);
          if (!prevShift) return false;
          const hours = (candidate.start.getTime() - prevShift.end.getTime()) / 36e5;
          if (hours < candidate.minRestHours) return false;
        }
      }
      const nextIdx = idx + 1;
      if (nextIdx < windowDays.length) {
        const nextCode = byDate.get(dateKeys[nextIdx]);
        if (isWorkCode(nextCode)) {
          const nextShift = getShiftStartEnd(dateKeys[nextIdx], nextCode);
          if (!nextShift) return false;
          const hours = (nextShift.start.getTime() - candidate.end.getTime()) / 36e5;
          if (hours < candidate.minRestHours) return false;
        }
      }
      return true;
    };

    const canPlaceShift = (empId, idx, shiftCode) => {
      const byDate = plan.get(empId);
      if (!byDate) return false;
      if (byDate.get(dateKeys[idx]) !== "") return false;
      if (lockedOff.has(lockKey(empId, idx))) return false;
      if (lockedShift.has(lockKey(empId, idx))) return false;
      if (!shiftByCode.has(shiftCode)) return false;
      if (String(shiftCode || "").includes("常夜") && !fixedNightEmpSet.has(empId)) return false;
      if (!minRestOk(empId, idx, shiftCode)) return false;
      return true;
    };

    const countOffInRange = (empId, startIdx, endIdx) => {
      const byDate = plan.get(empId);
      if (!byDate) return 0;
      let count = 0;
      for (let idx = startIdx; idx <= endIdx; idx += 1) {
        if (isOffCode(byDate.get(dateKeys[idx]))) count += 1;
      }
      return count;
    };

    const pickCandidate = (idx, shiftCode, options = {}) => {
      const candidates = [];
      for (const person of this.people) {
        if (options.exclude && options.exclude.has(person.empId)) continue;
        if (!canPlaceShift(person.empId, idx, shiftCode)) continue;
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
      plan.get(empId).set(dateKeys[idx], shiftCode);
      assignedCount.set(empId, (assignedCount.get(empId) || 0) + 1);
      if (shiftCode === coverageShiftCodes.night || shiftCode === coverageShiftCodes.longNight) {
        nightCount.set(empId, (nightCount.get(empId) || 0) + 1);
      }
    };

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

    // Stage 2.5: apply fixed shifts (non-LN) without breaking OFF
    for (const person of this.people) {
      const byDate = plan.get(person.empId);
      const fixedByDate = fixedShiftByEmp.get(person.empId);
      if (!byDate || !fixedByDate) continue;
      for (const [dateKey, shiftCode] of fixedByDate.entries()) {
        const idx = dayIndex.get(dateKey);
        if (idx === undefined) continue;
        if (isOffCode(byDate.get(dateKey))) continue;
        if (String(shiftCode || "").includes("常夜")) continue;
        if (byDate.get(dateKey) !== "") continue;
        if (canPlaceShift(person.empId, idx, shiftCode)) {
          byDate.set(dateKey, shiftCode);
          lockedShift.add(lockKey(person.empId, idx));
        } else {
          logWarn({ type: "fixed_shift_unassigned", empId: person.empId, date: dateKey, shiftCode });
        }
      }
    }

    for (const person of this.people) {
      const byDate = plan.get(person.empId);
      if (!byDate) continue;
      for (const { dateKey } of windowDays) {
        const code = byDate.get(dateKey);
        if (!isWorkCode(code)) continue;
        assignedCount.set(person.empId, (assignedCount.get(person.empId) || 0) + 1);
        if (isNightCode_(code)) {
          nightCount.set(person.empId, (nightCount.get(person.empId) || 0) + 1);
        }
      }
    }

    const dailyNightTaken = new Map(windowDays.map(item => [item.dateKey, false]));
    for (const { dateKey } of windowDays) {
      for (const person of this.people) {
        const code = plan.get(person.empId).get(dateKey);
        if (code === coverageShiftCodes.longNight) {
          dailyNightTaken.set(dateKey, true);
          break;
        }
      }
    }

    // Stage 3: fill night coverage N
    for (let idx = 0; idx < windowDays.length; idx += 1) {
      const dateKey = dateKeys[idx];
      const demand = getDailyDemand(dateKey);
      const needNTotal = fixedNightEmpSet.size >= demand.night ? 0 : demand.night;
      if (needNTotal <= 0) continue;
      if (dailyNightTaken.get(dateKey)) continue;
      for (let count = 0; count < needNTotal; count += 1) {
        const candidate = pickCandidate(idx, coverageShiftCodes.night, { exclude: fixedNightEmpSet });
        if (candidate) {
          markAssigned(candidate, idx, coverageShiftCodes.night);
          lockedShift.add(lockKey(candidate, idx));
          dailyNightTaken.set(dateKey, true);
        } else {
          logWarn({ type: "no_feasible_night", date: dateKey, reason: "minRest/leave/offCap" });
          break;
        }
      }
    }

    // Stage 4: fill early and noon coverage
    for (let idx = 0; idx < windowDays.length; idx += 1) {
      const dateKey = dateKeys[idx];
      const demand = getDailyDemand(dateKey);

      if (shiftByCode.has(coverageShiftCodes.early)) {
        let currentEarly = 0;
        for (const person of this.people) {
          if (plan.get(person.empId).get(dateKey) === coverageShiftCodes.early) currentEarly += 1;
        }
        for (let count = currentEarly; count < demand.early; count += 1) {
          const candidate = pickCandidate(idx, coverageShiftCodes.early);
          if (candidate) {
            markAssigned(candidate, idx, coverageShiftCodes.early);
            lockedShift.add(lockKey(candidate, idx));
          } else {
            logWarn({ type: "no_feasible_early", date: dateKey, reason: "minRest/leave/offCap" });
            break;
          }
        }
      }

      if (shiftByCode.has(coverageShiftCodes.noon)) {
        let currentNoon = 0;
        for (const person of this.people) {
          if (plan.get(person.empId).get(dateKey) === coverageShiftCodes.noon) currentNoon += 1;
        }
        for (let count = currentNoon; count < demand.noon; count += 1) {
          const candidate = pickCandidate(idx, coverageShiftCodes.noon);
          if (candidate) {
            markAssigned(candidate, idx, coverageShiftCodes.noon);
            lockedShift.add(lockKey(candidate, idx));
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
          if (isOffCode(code)) offCount += 1;
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
        const maxOff = dailyOffCap.get(dateKey) || 0;
        if (offCount > maxOff) {
          logWarn({ date: dateKey, type: "off_cap_exceeded", offCount, maxOff });
        }
      }

      for (const person of this.people) {
        const byDate = plan.get(person.empId);
        if (!byDate) continue;
        const quota = quotaByEmp.get(person.empId) || { quotaSun: 0, quotaSat: 0, quotaOffTotal: 0 };
        const sunCount = sundayIdx.reduce((sum, idx) => sum + (byDate.get(dateKeys[idx]) === "R_sun" ? 1 : 0), 0);
        const satCount = saturdayIdx.reduce((sum, idx) => sum + (byDate.get(dateKeys[idx]) === "R_sat" ? 1 : 0), 0);
        const offTotal = windowDays.reduce((sum, { dateKey }) => sum + (isOffCode(byDate.get(dateKey)) ? 1 : 0), 0);
        if (sunCount !== quota.quotaSun) {
          logViolation({ empId: person.empId, type: "quota_sun", details: `target=${quota.quotaSun} actual=${sunCount}` });
        }
        if (satCount !== quota.quotaSat) {
          logViolation({ empId: person.empId, type: "quota_sat", details: `target=${quota.quotaSat} actual=${satCount}` });
        }
        if (offTotal !== quota.quotaOffTotal) {
          logViolation({ empId: person.empId, type: "quota_off_total", details: `target=${quota.quotaOffTotal} actual=${offTotal}` });
        }

        for (const bucket of weekBucketsIdx) {
          const hasOff = bucket.some(idx => isOffCode(byDate.get(dateKeys[idx])));
          if (!hasOff) {
            logViolation({ empId: person.empId, type: "weekly_off_missing", details: `weekStart=${dateKeys[bucket[0]]}` });
          }
        }

        for (let idx = 1; idx < windowDays.length; idx += 1) {
          const prevKey = dateKeys[idx - 1];
          const curKey = dateKeys[idx];
          const prevCode = byDate.get(prevKey);
          const curCode = byDate.get(curKey);
          if (isWorkCode(prevCode) && isWorkCode(curCode)) {
            const prevShift = getShiftStartEnd(prevKey, prevCode);
            const curShift = getShiftStartEnd(curKey, curCode);
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
            if (isWorkCode(code) && code !== coverageShiftCodes.longNight) {
              logViolation({ empId: person.empId, type: "fixed_night", details: `date=${dateKey} code=${code}` });
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
        row.push(byDate ? byDate.get(dateKey) || "" : "");
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
        row.push(byDate ? byDate.get(dateKey) || "" : "");
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
