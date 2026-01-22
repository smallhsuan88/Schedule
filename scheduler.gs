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
    const monthIdxSet = new Set(days.map(({ dateKey }) => dayIndex.get(dateKey)).filter(idx => idx !== undefined));
    const monthIndices = [...monthIdxSet].sort((a, b) => a - b);
    const getDateKeyOffset = (dateKey, offset) => {
      const idx = dayIndex.get(dateKey);
      if (idx === undefined) return null;
      const nextIdx = idx + offset;
      if (nextIdx < 0 || nextIdx >= windowDays.length) return null;
      return windowDays[nextIdx].dateKey;
    };

    // 在任何情況下，不得為了塞滿 R/r 配額而讓某天 off 人數 > 2 或造成當天缺早/午/夜；若配額與 coverage 衝突，優先維持 daily coverage，並將無法安置的 R/r 以 warning 回報。
    const shiftByCode = new Map(Object.values(this.shiftDefs).map(def => [def.shiftCode, def]));
    const fixedRulesByEmp = new Map();
    for (const rule of this.fixedRules) {
      if (!fixedRulesByEmp.has(rule.empId)) fixedRulesByEmp.set(rule.empId, []);
      fixedRulesByEmp.get(rule.empId).push(rule);
    }
    const fixedNightEmpSet = new Set();
    for (const rule of this.fixedRules) {
      if (String(rule.shiftCode || "").includes("常夜")) {
        fixedNightEmpSet.add(rule.empId);
      }
    }
    const hasFixedNightRule = fixedNightEmpSet.size > 0;
    const leaveTypes = new Set(this.leave.map(item => item.leaveType).filter(Boolean));
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
    const isLeaveCode_ = code => {
      if (!code) return false;
      if (code === "R" || code === "r") return false;
      return leaveTypes.has(code) || !shiftByCode.has(code);
    };
    const isOffCode = code => code === "R" || code === "r" || isLeaveCode_(code);
    const isWorkCode = code => Boolean(code) && !isOffCode(code);
    const isNightCode_ = code => code === "夜" || code === "夜L" || String(code || "").includes("常夜");
    const isEarlyCode_ = code => code === "早" || code === "早L";
    const isNoonCode_ = code => code === "午" || code === "午L";
    const coverageShiftCodes = {
      early: shiftByCode.has("早L") ? "早L" : "早",
      noon: shiftByCode.has("午L") ? "午L" : "午",
      night: shiftByCode.has("夜L") ? "夜L" : "夜"
    };

    const plan = new Map();
    for (const person of this.people) {
      const byDate = new Map();
      for (const { dateKey } of windowDays) {
        byDate.set(dateKey, "");
      }
      plan.set(person.empId, byDate);
    }

    const weekBuckets = new Map();
    for (const { dateKey, date } of windowDays) {
      const weekStart = new Date(date);
      weekStart.setDate(weekStart.getDate() - weekStart.getDay());
      const weekKey = fmtDate(weekStart);
      if (!weekBuckets.has(weekKey)) weekBuckets.set(weekKey, []);
      weekBuckets.get(weekKey).push(dayIndex.get(dateKey));
    }
    const weekBucketsIdx = [...weekBuckets.values()];

    const sundayIdx = [];
    const saturdayIdx = [];
    for (let idx = 0; idx < windowDays.length; idx += 1) {
      const dayOfWeek = dow[idx];
      if (dayOfWeek === 0) sundayIdx.push(idx);
      if (dayOfWeek === 6) saturdayIdx.push(idx);
    }
    const windowSundayCount = sundayIdx.length;
    const windowSaturdayCount = saturdayIdx.length;

    const dayAssigned = {};
    const nightTaken = {};
    for (const { dateKey } of windowDays) {
      dayAssigned[dateKey] = { early: 0, noon: 0, night: 0 };
      nightTaken[dateKey] = false;
    }
    const lockedSlots = windowDays.map(() => ({ earlyEmp: null, noonEmp: null, nightEmp: null }));
    const lockedMap = new Map();
    const isLocked = (empId, idx) => lockedMap.has(`${empId}#${idx}`);
    const setLocked = (empId, idx, shiftCode) => {
      lockedMap.set(`${empId}#${idx}`, shiftCode);
      if (isNightCode_(shiftCode)) lockedSlots[idx].nightEmp = empId;
      if (isEarlyCode_(shiftCode)) lockedSlots[idx].earlyEmp = empId;
      if (isNoonCode_(shiftCode)) lockedSlots[idx].noonEmp = empId;
    };

    const findFixedShiftCode = (empId, dateKey) => {
      const rules = fixedRulesByEmp.get(empId) || [];
      for (const rule of rules) {
        const dates = expandDateRange(rule.dateFrom, rule.dateTo).map(fmtDate);
        if (dates.includes(dateKey)) return rule.shiftCode;
      }
      return null;
    };

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

    const countOffPeople = idx => {
      const dateKey = dateKeys[idx];
      let count = 0;
      for (const person of this.people) {
        const code = plan.get(person.empId).get(dateKey);
        if (isOffCode(code)) count += 1;
      }
      return count;
    };

    const countLeavePeople = idx => {
      const dateKey = dateKeys[idx];
      let count = 0;
      for (const person of this.people) {
        const code = plan.get(person.empId).get(dateKey);
        if (isLeaveCode_(code)) count += 1;
      }
      return count;
    };

    const assignedCount = new Map(this.people.map(p => [p.empId, 0]));
    const nightCount = new Map(this.people.map(p => [p.empId, 0]));

    const canPlaceShift = (empId, idx, candidateShiftCode, options = {}) => {
      const byDate = plan.get(empId);
      if (!byDate) return false;
      if (byDate.get(dateKeys[idx]) !== "") return false;
      if (!shiftByCode.has(candidateShiftCode)) return false;
      const fixedShift = findFixedShiftCode(empId, dateKeys[idx]);
      if (fixedShift && fixedShift !== candidateShiftCode && isWorkCode(candidateShiftCode)) {
        return false;
      }
      if (String(candidateShiftCode || "").includes("常夜") && !fixedNightEmpSet.has(empId)) {
        return false;
      }
      if (isLocked(empId, idx)) return false;
      if (isNightCode_(candidateShiftCode) && nightTaken[dateKeys[idx]] && !options.ignoreNightCap) return false;
      if (!minRestOk(empId, idx, candidateShiftCode)) return false;
      return true;
    };

    const pickCandidateForShift = (idx, shiftCode, excludeEmpIds = new Set()) => {
      const candidates = [];
      for (const person of this.people) {
        if (excludeEmpIds.has(person.empId)) continue;
        const byDate = plan.get(person.empId);
        if (!byDate || byDate.get(dateKeys[idx]) !== "") continue;
        if (!canPlaceShift(person.empId, idx, shiftCode)) continue;
        candidates.push(person.empId);
      }
      candidates.sort((a, b) => (assignedCount.get(a) || 0) - (assignedCount.get(b) || 0));
      return candidates[0] || null;
    };

    const pickCandidateForNight = idx => {
      const candidates = [];
      for (const person of this.people) {
        if (fixedNightEmpSet.has(person.empId)) continue;
        const byDate = plan.get(person.empId);
        if (!byDate || byDate.get(dateKeys[idx]) !== "") continue;
        if (!canPlaceShift(person.empId, idx, coverageShiftCodes.night)) continue;
        candidates.push(person.empId);
      }
      candidates.sort((a, b) => {
        const nightDiff = (nightCount.get(a) || 0) - (nightCount.get(b) || 0);
        if (nightDiff !== 0) return nightDiff;
        return (assignedCount.get(a) || 0) - (assignedCount.get(b) || 0);
      });
      return candidates[0] || null;
    };

    const restCountR = new Map(this.people.map(p => [p.empId, 0]));
    const restCountr = new Map(this.people.map(p => [p.empId, 0]));
    const dailyOffCap = new Map();

    const assignRest = (empId, idx, code) => {
      const dateKey = dateKeys[idx];
      const byDate = plan.get(empId);
      if (!byDate || byDate.get(dateKey) !== "") return false;
      if (this.leaveSet.has(`${empId}#${dateKey}`)) return false;
      if (isLocked(empId, idx)) return false;
      const offCap = dailyOffCap.get(dateKey);
      if (offCap !== undefined && countOffPeople(idx) >= offCap) return false;
      byDate.set(dateKey, code);
      if (code === "R") {
        restCountR.set(empId, (restCountR.get(empId) || 0) + 1);
      } else if (code === "r") {
        restCountr.set(empId, (restCountr.get(empId) || 0) + 1);
      }
      return true;
    };

    const moveRestWithinMonth_ = (empId, fromIdx, toIdx, code) => {
      const byDate = plan.get(empId);
      if (!byDate) return false;
      const fromKey = dateKeys[fromIdx];
      const toKey = dateKeys[toIdx];
      if (byDate.get(fromKey) !== code) return false;
      if (byDate.get(toKey) !== "") return false;
      if (this.leaveSet.has(`${empId}#${toKey}`)) return false;
      if (isLocked(empId, toIdx)) return false;
      const offCap = dailyOffCap.get(toKey);
      if (offCap !== undefined && countOffPeople(toIdx) >= offCap) return false;
      byDate.set(fromKey, "");
      byDate.set(toKey, code);
      return true;
    };

    const markAssigned = (dateKey, shiftCode, empId) => {
      if (isEarlyCode_(shiftCode)) dayAssigned[dateKey].early += 1;
      if (isNoonCode_(shiftCode)) dayAssigned[dateKey].noon += 1;
      if (isNightCode_(shiftCode)) {
        dayAssigned[dateKey].night += 1;
        nightTaken[dateKey] = true;
        nightCount.set(empId, (nightCount.get(empId) || 0) + 1);
      }
      assignedCount.set(empId, (assignedCount.get(empId) || 0) + 1);
    };

    const placeShift = (empId, idx, shiftCode, options = {}) => {
      const dateKey = dateKeys[idx];
      if (!shiftByCode.has(shiftCode)) return false;
      if (plan.get(empId).get(dateKey) !== "") return false;
      if (String(shiftCode || "").includes("常夜") && !fixedNightEmpSet.has(empId)) return false;
      if (!canPlaceShift(empId, idx, shiftCode, options)) return false;
      if (isNightCode_(shiftCode) && nightTaken[dateKey] && !options.ignoreNightCap) return false;
      plan.get(empId).set(dateKey, shiftCode);
      markAssigned(dateKey, shiftCode, empId);
      return true;
    };

    // Phase A: seed existing schedule (window context)
    const seedMatrix = options.seedMatrix instanceof Map ? options.seedMatrix : null;
    if (seedMatrix) {
      for (const [empId, byDateSeed] of seedMatrix.entries()) {
        const byDate = plan.get(empId);
        if (!byDate) continue;
        for (const [dateKey, code] of byDateSeed.entries()) {
          if (!byDate.has(dateKey)) continue;
          if (byDate.get(dateKey) !== "") continue;
          if (!code) continue;
          byDate.set(dateKey, code);
        }
      }
    }

    // Phase B: Leave (do not overwrite)
    for (const item of this.leave) {
      const dateKey = fmtDate(item.date);
      const byDate = plan.get(item.empId);
      if (byDate && byDate.has(dateKey)) {
        byDate.set(dateKey, item.leaveType);
      }
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
      const leaveCount = countLeavePeople(idx);
      dailyLeaveCounts.set(dateKey, leaveCount);
      const demand = monthIdxSet.has(idx) ? getDailyDemand(dateKey) : { early: 0, noon: 0, night: 0, total: 0 };
      const offCap = Math.max(0, this.people.length - (demand.total || 0));
      dailyOffCap.set(dateKey, offCap);
      if (monthIdxSet.has(idx) && this.people.length - leaveCount < (demand.total || 0)) {
        Logger.log(`[WARN] coverage impossible date=${dateKey} leaveCount=${leaveCount} demand=${demand.total || 0}`);
      }
    }

    // Phase C: lock daily coverage slots (early/noon/night) for month days
    for (const idx of monthIndices) {
      const dateKey = dateKeys[idx];
      const demand = getDailyDemand(dateKey);
      const leaveCount = dailyLeaveCounts.get(dateKey) || 0;
      for (const person of this.people) {
        const fixedShift = findFixedShiftCode(person.empId, dateKey);
        if (!fixedShift) continue;
        if (this.leaveSet.has(`${person.empId}#${dateKey}`)) continue;
        if (plan.get(person.empId).get(dateKey) !== "") continue;
        if (placeShift(person.empId, idx, fixedShift)) {
          setLocked(person.empId, idx, fixedShift);
        } else {
          Logger.log(`[WARN] cannot place fixed shift empId=${person.empId} date=${dateKey} shift=${fixedShift}`);
        }
      }
      let nightLocked = nightTaken[dateKey] || false;
      if (hasFixedNightRule) {
        for (const empId of fixedNightEmpSet) {
          const byDate = plan.get(empId);
          if (byDate && byDate.get(dateKey) === "" && !this.leaveSet.has(`${empId}#${dateKey}`)) {
            if (shiftByCode.has("常夜") && placeShift(empId, idx, "常夜")) {
              nightLocked = true;
              setLocked(empId, idx, "常夜");
              break;
            }
          }
        }
      }

      if (!nightLocked && demand.night > 0) {
        if (shiftByCode.has(coverageShiftCodes.night)) {
          const nightCandidate = pickCandidateForNight(idx);
          if (nightCandidate) {
            placeShift(nightCandidate, idx, coverageShiftCodes.night);
            setLocked(nightCandidate, idx, coverageShiftCodes.night);
            nightLocked = true;
          } else if (leaveCount <= (dailyOffCap.get(dateKey) || 0)) {
            Logger.log(`[WARN] missing night coverage date=${dateKey}`);
          }
        }
      }

      if (shiftByCode.has(coverageShiftCodes.early) && demand.early > 0) {
        const earlyCandidate = pickCandidateForShift(idx, coverageShiftCodes.early);
        if (earlyCandidate) {
          placeShift(earlyCandidate, idx, coverageShiftCodes.early);
          setLocked(earlyCandidate, idx, coverageShiftCodes.early);
        } else if (leaveCount <= (dailyOffCap.get(dateKey) || 0)) {
          Logger.log(`[WARN] missing early coverage date=${dateKey}`);
        }
      }

      if (shiftByCode.has(coverageShiftCodes.noon) && demand.noon > 0) {
        const noonCandidate = pickCandidateForShift(idx, coverageShiftCodes.noon);
        if (noonCandidate) {
          placeShift(noonCandidate, idx, coverageShiftCodes.noon);
          setLocked(noonCandidate, idx, coverageShiftCodes.noon);
        } else if (leaveCount <= (dailyOffCap.get(dateKey) || 0)) {
          Logger.log(`[WARN] missing noon coverage date=${dateKey}`);
        }
      }
    }

    // Phase D: place monthly R/r quotas (only on unassigned, non-leave slots)
    for (const person of this.people) {
      const missingSundays = [];
      for (const idx of sundayIdx) {
        if (!assignRest(person.empId, idx, "R")) missingSundays.push(idx);
      }
      for (const idx of missingSundays) {
        const bucket = weekBucketsIdx.find(week => week.includes(idx)) || [];
        const fallbackIdx = bucket.find(candidateIdx => assignRest(person.empId, candidateIdx, "R"));
        if (fallbackIdx === undefined) {
          // keep for later fallback
        }
      }
      for (const idx of saturdayIdx) {
        if (!assignRest(person.empId, idx, "r")) {
          const bucket = weekBucketsIdx.find(week => week.includes(idx)) || [];
          bucket.find(candidateIdx => assignRest(person.empId, candidateIdx, "r"));
        }
      }

      for (let idx = 0; idx < windowDays.length; idx += 1) {
        if ((restCountR.get(person.empId) || 0) >= windowSundayCount) break;
        assignRest(person.empId, idx, "R");
      }
      for (let idx = 0; idx < windowDays.length; idx += 1) {
        if ((restCountr.get(person.empId) || 0) >= windowSaturdayCount) break;
        if (plan.get(person.empId).get(dateKeys[idx]) === "R") continue;
        assignRest(person.empId, idx, "r");
      }
      const missingR = windowSundayCount - (restCountR.get(person.empId) || 0);
      const missingr = windowSaturdayCount - (restCountr.get(person.empId) || 0);
      if (missingR > 0 || missingr > 0) {
        Logger.log(`[WARN] cannot place monthly R/r quota empId=${person.empId} missingR=${Math.max(0, missingR)} missingr=${Math.max(0, missingr)}`);
      }
    }

    // Phase D2: weekly R (at least 1 per week)
    for (const person of this.people) {
      const byDate = plan.get(person.empId);
      if (!byDate) continue;
      for (const bucket of weekBucketsIdx) {
        const hasR = bucket.some(idx => byDate.get(dateKeys[idx]) === "R");
        if (hasR) continue;
        let placed = false;
        if ((restCountR.get(person.empId) || 0) < windowSundayCount) {
          const targetIdx = bucket.find(idx => assignRest(person.empId, idx, "R"));
          placed = targetIdx !== undefined;
        }
        if (!placed) {
          let moved = false;
          for (const donorBucket of weekBucketsIdx) {
            const donorR = donorBucket.filter(idx => byDate.get(dateKeys[idx]) === "R");
            if (donorR.length <= 1) continue;
            const slotIdx = bucket.find(idx => byDate.get(dateKeys[idx]) === "" && !this.leaveSet.has(`${person.empId}#${dateKeys[idx]}`));
            if (slotIdx === undefined) continue;
            if (moveRestWithinMonth_(person.empId, donorR[0], slotIdx, "R")) {
              moved = true;
              break;
            }
          }
          if (!moved) {
            Logger.log(`[WARN] cannot place weekly R for empId=${person.empId} week=${dateKeys[bucket[0]]}`);
          }
        }
      }
    }

    // Phase D3: any 7-day window must have R/r (window-based)
    for (const person of this.people) {
      const byDate = plan.get(person.empId);
      if (!byDate) continue;
      for (let startIdx = 0; startIdx <= windowDays.length - 7; startIdx += 1) {
        const endIdx = startIdx + 6;
        let hasRest = false;
        for (let idx = startIdx; idx <= endIdx; idx += 1) {
          const code = byDate.get(dateKeys[idx]);
          if (code === "R" || code === "r") {
            hasRest = true;
            break;
          }
        }
        if (hasRest) continue;
        let targetIdx = null;
        for (let idx = startIdx; idx <= endIdx; idx += 1) {
          const offCap = dailyOffCap.get(dateKeys[idx]);
          if (plan.get(person.empId).get(dateKeys[idx]) === "" && (offCap === undefined || countOffPeople(idx) < offCap)) {
            targetIdx = idx;
            break;
          }
        }
        if (targetIdx === null) {
          Logger.log(`[WARN] cannot enforce 7-day rest window empId=${person.empId} range=${dateKeys[startIdx]}~${dateKeys[endIdx]}`);
          continue;
        }

        if ((restCountr.get(person.empId) || 0) < windowSaturdayCount) {
          if (!assignRest(person.empId, targetIdx, "r")) {
            Logger.log(`[WARN] cannot place r for 7-day rest window empId=${person.empId} range=${dateKeys[startIdx]}~${dateKeys[endIdx]}`);
          }
          continue;
        }

        let moved = false;
        for (let idx = 0; idx < windowDays.length; idx += 1) {
          if (idx >= startIdx && idx <= endIdx) continue;
          if (byDate.get(dateKeys[idx]) === "r") {
            if (moveRestWithinMonth_(person.empId, idx, targetIdx, "r")) {
              moved = true;
              break;
            }
          }
        }
        if (!moved && (restCountR.get(person.empId) || 0) < windowSundayCount) {
          if (assignRest(person.empId, targetIdx, "R")) moved = true;
        }
        if (!moved) {
          Logger.log(`[WARN] cannot move rest into 7-day rest window empId=${person.empId} range=${dateKeys[startIdx]}~${dateKeys[endIdx]}`);
        }
      }
    }

    // Phase E: fill remaining shifts (no night beyond night-cap) for month days
    for (const person of this.people) {
      const byDate = plan.get(person.empId);
      if (!byDate) continue;
      for (const idx of monthIndices) {
        const dateKey = dateKeys[idx];
        if (byDate.get(dateKey) !== "") continue;
        let placed = false;
        const fixedShift = findFixedShiftCode(person.empId, dateKey);
        if (fixedShift && shiftByCode.has(fixedShift)) {
          if (placeShift(person.empId, idx, fixedShift)) placed = true;
        } else if (fixedNightEmpSet.has(person.empId) && shiftByCode.has("常夜")) {
          if (placeShift(person.empId, idx, "常夜")) placed = true;
        }
        if (!placed) {
          if (shiftByCode.has(coverageShiftCodes.early) && placeShift(person.empId, idx, coverageShiftCodes.early)) {
            placed = true;
          } else if (shiftByCode.has(coverageShiftCodes.noon) && placeShift(person.empId, idx, coverageShiftCodes.noon)) {
            placed = true;
          } else if (shiftByCode.has(coverageShiftCodes.night) && placeShift(person.empId, idx, coverageShiftCodes.night)) {
            placed = true;
          }
        }
        if (!placed) {
          Logger.log(`[WARN] no feasible shift due to minRest/nightCap empId=${person.empId} date=${dateKey}`);
        }
      }
    }

    const validatePlanDetailed = () => {
      const violations = [];
      const warnings = [];
      const coverage = [];
      for (const { dateKey } of days) {
        let earlyCount = 0;
        let noonCount = 0;
        let nightCount = 0;
        let offCount = 0;
        const leaveCount = dailyLeaveCounts.get(dateKey) || 0;
        const demand = getDailyDemand(dateKey);
        for (const person of this.people) {
          const code = plan.get(person.empId).get(dateKey);
          if (isOffCode(code)) offCount += 1;
          if (isEarlyCode_(code)) earlyCount += 1;
          if (isNoonCode_(code)) noonCount += 1;
          if (isNightCode_(code)) nightCount += 1;
        }
        coverage.push({ date: dateKey, earlyCount, noonCount, nightCount, offCount });
        if (earlyCount < demand.early || noonCount < demand.noon || nightCount < demand.night || nightCount > 1) {
          const entry = { date: dateKey, type: "daily_coverage", details: `early=${earlyCount} noon=${noonCount} night=${nightCount}`, leaveCount, demand };
          if (leaveCount > (dailyOffCap.get(dateKey) || 0)) {
            warnings.push(entry);
          } else {
            violations.push(entry);
          }
        }
        const offCap = dailyOffCap.get(dateKey) || 0;
        if (offCount > offCap && leaveCount <= offCap) {
          violations.push({ date: dateKey, type: "daily_off_cap", details: `offCount=${offCount} offCap=${offCap}` });
        }
        if (nightCount > 1) {
          violations.push({ date: dateKey, type: "night_cap", details: `nightCount=${nightCount}` });
        }
      }

      for (const person of this.people) {
        const byDate = plan.get(person.empId);
        if (!byDate) continue;
        const rTotal = windowDays.reduce((sum, { dateKey }) => sum + (byDate.get(dateKey) === "r" ? 1 : 0), 0);
        const RTotal = windowDays.reduce((sum, { dateKey }) => sum + (byDate.get(dateKey) === "R" ? 1 : 0), 0);
        if (RTotal !== windowSundayCount) {
          violations.push({ empId: person.empId, type: "monthly_R_count", details: `target=${windowSundayCount} actual=${RTotal}` });
        }
        if (rTotal !== windowSaturdayCount) {
          violations.push({ empId: person.empId, type: "monthly_r_count", details: `target=${windowSaturdayCount} actual=${rTotal}` });
        }

        for (const bucket of weekBucketsIdx) {
          const hasR = bucket.some(idx => byDate.get(dateKeys[idx]) === "R");
          if (!hasR) {
            violations.push({ empId: person.empId, type: "weekly_R_missing", details: `weekStart=${dateKeys[bucket[0]]}` });
          }
        }

        for (let startIdx = 0; startIdx <= windowDays.length - 7; startIdx += 1) {
          const endIdx = startIdx + 6;
          let hasRest = false;
          for (let idx = startIdx; idx <= endIdx; idx += 1) {
            const code = byDate.get(dateKeys[idx]);
            if (code === "R" || code === "r") {
              hasRest = true;
              break;
            }
          }
          if (!hasRest) {
            violations.push({ empId: person.empId, type: "seven_day_rest_missing", details: `range=${dateKeys[startIdx]}~${dateKeys[endIdx]}` });
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
              violations.push({ empId: person.empId, type: "min_rest", details: `prevDate=${prevKey} curDate=${curKey}` });
            } else {
              const hours = (curShift.start.getTime() - prevShift.end.getTime()) / 36e5;
              if (hours < (Number(curShift.minRestHours) || 11)) {
                violations.push({ empId: person.empId, type: "min_rest", details: `prevDate=${prevKey} curDate=${curKey}` });
              }
            }
          }
        }

        if (fixedNightEmpSet.has(person.empId)) {
          for (const { dateKey } of windowDays) {
            const code = byDate.get(dateKey);
            if (isWorkCode(code) && code !== "常夜") {
              violations.push({ empId: person.empId, type: "fixed_night", details: `date=${dateKey} code=${code}` });
            }
          }
        }
      }

      for (const { dateKey } of days) {
        const nightEmpIds = [];
        for (const person of this.people) {
          const code = plan.get(person.empId).get(dateKey);
          if (String(code || "").includes("常夜") && !fixedNightEmpSet.has(person.empId)) {
            violations.push({ empId: person.empId, date: dateKey, type: "fixed_night_violation", details: `code=${code}` });
          }
          if (isNightCode_(code)) nightEmpIds.push(person.empId);
        }
        if (nightEmpIds.length > 1) {
          violations.push({ date: dateKey, type: "night_cap", details: `nightEmpIds=${nightEmpIds.join(",")}` });
        }
      }

      return { ok: violations.length === 0, violations, warnings, coverage };
    };

    const validation = validatePlanDetailed();
    if (!validation.ok) {
      for (const violation of validation.violations) {
        Logger.log(`[VIOLATION] ${JSON.stringify(violation)}`);
      }
    }
    for (const warning of validation.warnings || []) {
      if (warning.type === "daily_coverage" && warning.leaveCount > 2) {
        Logger.log(`[WARN] coverage impossible due to leave>2 date=${warning.date} leaveCount=${warning.leaveCount} missing=${warning.details}`);
      }
      Logger.log(`[WARN] ${JSON.stringify(warning)}`);
    }

    this.monthPlan = { month, days, plan };
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
