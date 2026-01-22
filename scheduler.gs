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

  buildMonthPlan(month) {
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

    const start = new Date(year, monthIndex, 1);
    const end = new Date(year, monthIndex + 1, 0);
    const daysInMonth = end.getDate();

    const days = [];
    for (let day = 1; day <= daysInMonth; day += 1) {
      const date = new Date(year, monthIndex, day);
      days.push({ day, dateKey: fmtDate(date), date });
    }

    // Phase A: calendar structure
    const dates = days.map(({ date }) => date);
    const dateKeys = days.map(({ dateKey }) => dateKey);
    const dow = dates.map(date => dow_(date));

    const dayIndex = new Map(days.map((item, idx) => [item.dateKey, idx]));
    const getDateKeyOffset = (dateKey, offset) => {
      const idx = dayIndex.get(dateKey);
      if (idx === undefined) return null;
      const nextIdx = idx + offset;
      if (nextIdx < 0 || nextIdx >= days.length) return null;
      return days[nextIdx].dateKey;
    };

    const shiftByCode = new Map(Object.values(this.shiftDefs).map(def => [def.shiftCode, def]));
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
    const shiftStartEnd_ = (dateKey, shiftCode) => {
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
    const isWorkShift_ = code => {
      if (!code) return false;
      if (code === "R" || code === "r") return false;
      const def = shiftByCode.get(code);
      if (!def) return false;
      return String(def.isOff || "").toUpperCase() !== "Y";
    };
    const isLeaveCode_ = code => {
      if (!code) return false;
      if (code === "R" || code === "r") return false;
      return !shiftByCode.has(code);
    };
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
      for (const { dateKey } of days) {
        byDate.set(dateKey, "");
      }
      plan.set(person.empId, byDate);
    }

    const monthStart = new Date(year, monthIndex, 1);
    const weekBuckets = [];
    const weekBucketsMap = new Map();
    for (const { date } of days) {
      const idx = weekIndex_(date, monthStart);
      if (!weekBucketsMap.has(idx)) weekBucketsMap.set(idx, []);
      weekBucketsMap.get(idx).push(date);
    }
    for (const weekDates of weekBucketsMap.values()) {
      weekBuckets.push(weekDates);
    }
    const weekBucketsIdx = weekBuckets.map(weekDates => weekDates.map(date => dayIndex.get(fmtDate(date))));
    const weekIndexByDate = new Array(days.length).fill(0);
    weekBucketsIdx.forEach((bucket, bucketIdx) => {
      for (const idx of bucket) weekIndexByDate[idx] = bucketIdx;
    });
    const sundayIdx = [];
    const saturdayIdx = [];
    dow.forEach((dayOfWeek, idx) => {
      if (dayOfWeek === 0) sundayIdx.push(idx);
      if (dayOfWeek === 6) saturdayIdx.push(idx);
    });
    const monthSundayCount = sundayIdx.length;
    const monthSaturdayCount = saturdayIdx.length;

    const dayNeed = {};
    const dayAssigned = {};
    const nightTaken = {};
    const fixedNightEmpSet = new Set(
      this.fixedRules.filter(rule => String(rule.shiftCode || "").includes("常夜")).map(rule => rule.empId)
    );
    for (const { dateKey } of days) {
      dayNeed[dateKey] = { early: 1, noon: 1, night: 1 };
      dayAssigned[dateKey] = { early: 0, noon: 0, night: 0 };
      nightTaken[dateKey] = false;
    }

    const getPreferredShift = (empId, dateKey) => {
      for (const rule of this.fixedRules) {
        if (rule.empId !== empId) continue;
        const dates = expandDateRange(rule.dateFrom, rule.dateTo).map(fmtDate);
        if (dates.includes(dateKey)) return rule.shiftCode || null;
      }
      return null;
    };

    const minRestOk_ = (prevDateKey, prevCode, curDateKey, curCode, minHours = 11) => {
      if (!isWorkShift_(prevCode) || !isWorkShift_(curCode)) return true;
      const prevShift = shiftStartEnd_(prevDateKey, prevCode);
      const curShift = shiftStartEnd_(curDateKey, curCode);
      if (!prevShift || !curShift) return false;
      const hours = (curShift.start.getTime() - prevShift.end.getTime()) / 36e5;
      return hours >= minHours;
    };

    const canPlaceShift_ = (empId, dateKey, candidateShiftCode, options = {}) => {
      const byDate = plan.get(empId);
      if (!byDate) return true;
      const candidate = shiftStartEnd_(dateKey, candidateShiftCode);
      if (!candidate) return false;
      if (isNightCode_(candidateShiftCode) && nightTaken[dateKey] && !options.ignoreNightCap) return false;

      const prevDateKey = getDateKeyOffset(dateKey, -1);
      if (prevDateKey) {
        const prevCode = byDate.get(prevDateKey);
        if (!minRestOk_(prevDateKey, prevCode, dateKey, candidateShiftCode, candidate.minRestHours)) return false;
      }

      const nextDateKey = getDateKeyOffset(dateKey, 1);
      if (nextDateKey) {
        const nextCode = byDate.get(nextDateKey);
        if (!minRestOk_(dateKey, candidateShiftCode, nextDateKey, nextCode, candidate.minRestHours)) return false;
      }
      return true;
    };

    const restCountR = new Map(this.people.map(p => [p.empId, 0]));
    const restCountr = new Map(this.people.map(p => [p.empId, 0]));

    const assignRest = (empId, idx, code) => {
      const dateKey = dateKeys[idx];
      const byDate = plan.get(empId);
      if (!byDate || byDate.get(dateKey) !== "") return false;
      if (this.leaveSet.has(`${empId}#${dateKey}`)) return false;
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
      byDate.set(fromKey, "");
      byDate.set(toKey, code);
      return true;
    };

    const markAssigned = (dateKey, shiftCode) => {
      if (isEarlyCode_(shiftCode)) dayAssigned[dateKey].early += 1;
      if (isNoonCode_(shiftCode)) dayAssigned[dateKey].noon += 1;
      if (isNightCode_(shiftCode)) {
        dayAssigned[dateKey].night += 1;
        nightTaken[dateKey] = true;
      }
    };

    const placeShift = (empId, dateKey, shiftCode) => {
      if (!shiftByCode.has(shiftCode)) return false;
      if (plan.get(empId).get(dateKey) !== "") return false;
      if (String(shiftCode || "").includes("常夜") && !fixedNightEmpSet.has(empId)) return false;
      if (!canPlaceShift_(empId, dateKey, shiftCode)) return false;
      if (isNightCode_(shiftCode) && nightTaken[dateKey]) return false;
      plan.get(empId).set(dateKey, shiftCode);
      markAssigned(dateKey, shiftCode);
      return true;
    };

    // Phase B: Leave
    for (const item of this.leave) {
      const dateKey = fmtDate(item.date);
      const byDate = plan.get(item.empId);
      if (byDate && byDate.has(dateKey)) {
        byDate.set(dateKey, item.leaveType);
      }
    }

    // Phase C1: place monthly R/r quotas
    for (const person of this.people) {
      const missingSundays = [];
      for (const idx of sundayIdx) {
        if (!assignRest(person.empId, idx, "R")) missingSundays.push(idx);
      }
      for (const idx of missingSundays) {
        const bucket = weekBucketsIdx[weekIndexByDate[idx]] || [];
        const fallbackIdx = bucket.find(candidateIdx => assignRest(person.empId, candidateIdx, "R"));
        if (fallbackIdx === undefined) {
          // keep for later fallback
        }
      }
      for (const idx of saturdayIdx) {
        if (!assignRest(person.empId, idx, "r")) {
          const bucket = weekBucketsIdx[weekIndexByDate[idx]] || [];
          bucket.find(candidateIdx => assignRest(person.empId, candidateIdx, "r"));
        }
      }

      for (let idx = 0; idx < days.length && (restCountR.get(person.empId) || 0) < monthSundayCount; idx += 1) {
        assignRest(person.empId, idx, "R");
      }
      for (let idx = 0; idx < days.length && (restCountr.get(person.empId) || 0) < monthSaturdayCount; idx += 1) {
        if (plan.get(person.empId).get(dateKeys[idx]) === "R") continue;
        assignRest(person.empId, idx, "r");
      }
      if ((restCountR.get(person.empId) || 0) < monthSundayCount) {
        Logger.log(`[WARN] cannot fill R quota for empId=${person.empId} target=${monthSundayCount} actual=${restCountR.get(person.empId) || 0}`);
      }
      if ((restCountr.get(person.empId) || 0) < monthSaturdayCount) {
        Logger.log(`[WARN] cannot fill r quota for empId=${person.empId} target=${monthSaturdayCount} actual=${restCountr.get(person.empId) || 0}`);
      }
    }

    // Phase C2: weekly R (at least 1 per week)
    for (const person of this.people) {
      const byDate = plan.get(person.empId);
      if (!byDate) continue;
      for (const bucket of weekBucketsIdx) {
        const hasR = bucket.some(idx => byDate.get(dateKeys[idx]) === "R");
        if (hasR) continue;
        const targetIdx = bucket.find(idx => byDate.get(dateKeys[idx]) === "" && !this.leaveSet.has(`${person.empId}#${dateKeys[idx]}`));
        if (targetIdx === undefined) {
          Logger.log(`[WARN] cannot place weekly R for empId=${person.empId} week=${dateKeys[bucket[0]]}`);
          continue;
        }

        let moved = false;
        for (const donorBucket of weekBucketsIdx) {
          const donorR = donorBucket.filter(idx => byDate.get(dateKeys[idx]) === "R");
          if (donorR.length <= 1) continue;
          if (moveRestWithinMonth_(person.empId, donorR[0], targetIdx, "R")) {
            moved = true;
            break;
          }
        }
        if (!moved) {
          Logger.log(`[WARN] cannot move weekly R for empId=${person.empId} week=${dateKeys[bucket[0]]}`);
        }
      }
    }

    // Phase C3: any 7-day window must have R/r
    for (const person of this.people) {
      const byDate = plan.get(person.empId);
      if (!byDate) continue;
      for (let startIdx = 0; startIdx <= days.length - 7; startIdx += 1) {
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
        const targetIdx = (() => {
          for (let idx = startIdx; idx <= endIdx; idx += 1) {
            const dateKey = dateKeys[idx];
            if (byDate.get(dateKey) === "" && !this.leaveSet.has(`${person.empId}#${dateKey}`)) return idx;
          }
          return null;
        })();
        if (targetIdx === null) {
          Logger.log(`[WARN] cannot enforce 7-day rest window empId=${person.empId} range=${dateKeys[startIdx]}~${dateKeys[endIdx]}`);
          continue;
        }

        if ((restCountr.get(person.empId) || 0) < monthSaturdayCount) {
          assignRest(person.empId, targetIdx, "r");
          continue;
        }

        let moved = false;
        for (let idx = 0; idx < days.length; idx += 1) {
          if (idx >= startIdx && idx <= endIdx) continue;
          if (byDate.get(dateKeys[idx]) === "r") {
            if (moveRestWithinMonth_(person.empId, idx, targetIdx, "r")) {
              moved = true;
              break;
            }
          }
        }
        if (!moved) {
          Logger.log(`[WARN] cannot move r into 7-day rest window empId=${person.empId} range=${dateKeys[startIdx]}~${dateKeys[endIdx]}`);
        }
      }
    }

    const generalShiftPool = [coverageShiftCodes.early, coverageShiftCodes.noon, coverageShiftCodes.night]
      .filter(code => shiftByCode.has(code));
    if (!generalShiftPool.length) {
      for (const def of Object.values(this.shiftDefs)) {
        if (String(def.isOff || "").toUpperCase() === "Y") continue;
        if (String(def.shiftCode || "").includes("常夜")) continue;
        if (!generalShiftPool.includes(def.shiftCode)) generalShiftPool.push(def.shiftCode);
      }
    }

    // Phase D1: fixed night preference
    if (shiftByCode.has("常夜")) {
      for (const person of this.people) {
        if (!fixedNightEmpSet.has(person.empId)) continue;
        const byDate = plan.get(person.empId);
        if (!byDate) continue;
        for (const { dateKey } of days) {
          if (byDate.get(dateKey) !== "") continue;
          placeShift(person.empId, dateKey, "常夜");
        }
      }
    }

    // Phase D2: daily coverage (early/noon/night)
    for (const { dateKey } of days) {
      const available = () => this.people
        .map(p => p.empId)
        .filter(empId => !this.leaveSet.has(`${empId}#${dateKey}`))
        .filter(empId => plan.get(empId).get(dateKey) === "");

      const fillCoverage = (shiftCode, options = {}) => {
        const candidates = available()
          .filter(empId => (options.requireFixedNight ? fixedNightEmpSet.has(empId) : true));
        for (const empId of candidates) {
          if (placeShift(empId, dateKey, shiftCode)) return true;
        }
        return false;
      };

      const assigned = dayAssigned[dateKey];
      if (assigned.early < dayNeed[dateKey].early) {
        if (!shiftByCode.has(coverageShiftCodes.early) || !fillCoverage(coverageShiftCodes.early)) {
          Logger.log(`[WARN] coverage unmet date=${dateKey} missing=early`);
        }
      }
      if (assigned.noon < dayNeed[dateKey].noon) {
        if (!shiftByCode.has(coverageShiftCodes.noon) || !fillCoverage(coverageShiftCodes.noon)) {
          Logger.log(`[WARN] coverage unmet date=${dateKey} missing=noon`);
        }
      }
      if (assigned.night < dayNeed[dateKey].night) {
        let nightFilled = false;
        if (shiftByCode.has("常夜")) {
          nightFilled = fillCoverage("常夜", { requireFixedNight: true });
        }
        if (!nightFilled) {
          if (!shiftByCode.has(coverageShiftCodes.night) || !fillCoverage(coverageShiftCodes.night)) {
            Logger.log(`[WARN] coverage unmet date=${dateKey} missing=night`);
          }
        }
      }
    }

    // Phase D3: fill remaining shifts
    for (const person of this.people) {
      const byDate = plan.get(person.empId);
      if (!byDate) continue;
      for (const { dateKey } of days) {
        if (byDate.get(dateKey) !== "") continue;
        let placed = false;
        if (fixedNightEmpSet.has(person.empId) && shiftByCode.has("常夜")) {
          if (placeShift(person.empId, dateKey, "常夜")) placed = true;
        }
        if (!placed) {
          for (const code of generalShiftPool) {
            if (placeShift(person.empId, dateKey, code)) {
              placed = true;
              break;
            }
          }
        }
        if (!placed) {
          Logger.log(`[WARN] no feasible shift due to minRest/nightCap empId=${person.empId} date=${dateKey}`);
        }
      }
    }

    const validatePlan = () => {
      for (const person of this.people) {
        const byDate = plan.get(person.empId);
        if (!byDate) continue;
        const rTotal = days.reduce((sum, { dateKey }) => sum + (byDate.get(dateKey) === "r" ? 1 : 0), 0);
        const RTotal = days.reduce((sum, { dateKey }) => sum + (byDate.get(dateKey) === "R" ? 1 : 0), 0);
        if (RTotal !== monthSundayCount) {
          Logger.log(`[VIOLATION] empId=${person.empId} type=monthly_R_count details=target=${monthSundayCount} actual=${RTotal}`);
        }
        if (rTotal !== monthSaturdayCount) {
          Logger.log(`[VIOLATION] empId=${person.empId} type=monthly_r_count details=target=${monthSaturdayCount} actual=${rTotal}`);
        }

        for (const bucket of weekBucketsIdx) {
          const hasR = bucket.some(idx => byDate.get(dateKeys[idx]) === "R");
          if (!hasR) {
            Logger.log(`[VIOLATION] empId=${person.empId} type=weekly_R_missing details=weekStart=${dateKeys[bucket[0]]}`);
          }
        }

        for (let startIdx = 0; startIdx <= days.length - 7; startIdx += 1) {
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
            Logger.log(`[VIOLATION] empId=${person.empId} type=seven_day_rest_missing details=range=${dateKeys[startIdx]}~${dateKeys[endIdx]}`);
          }
        }

        for (let idx = 1; idx < days.length; idx += 1) {
          const prevKey = dateKeys[idx - 1];
          const curKey = dateKeys[idx];
          const prevCode = byDate.get(prevKey);
          const curCode = byDate.get(curKey);
          if (!minRestOk_(prevKey, prevCode, curKey, curCode)) {
            Logger.log(`[VIOLATION] empId=${person.empId} type=min_rest details=prevDate=${prevKey} curDate=${curKey}`);
          }
        }
      }
    };

    validatePlan();

    this.monthPlan = { month, days, plan };
    return this.monthPlan;
  }

  toMonthMatrix() {
    if (!this.monthPlan) throw new Error("Month plan not built");
    const headers = ["empId", "name", ...this.monthPlan.days.map(d => d.day)];
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
