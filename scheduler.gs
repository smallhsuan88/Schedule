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
    const isOffCode_ = code => {
      if (!code) return true;
      if (code === "R" || code === "r") return true;
      const def = shiftByCode.get(code);
      if (!def) return true;
      return String(def.isOff || "").toUpperCase() === "Y";
    };
    const isLeaveCode_ = code => {
      if (!code) return false;
      if (code === "R" || code === "r") return false;
      return !shiftByCode.has(code);
    };
    const isNightCode_ = code => code === "夜" || code === "常夜";
    const isEarlyCode_ = code => code === "早";
    const isNoonCode_ = code => code === "午";

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

    const dates = days.map(({ date }) => date);
    const saturdays = days.filter(({ date }) => dow_(date) === 6).map(({ date }) => date);
    const nightKeySet = new Set();
    const fixedNightEmpSet = new Set(
      this.fixedRules.filter(rule => String(rule.shiftCode || "").includes("常夜")).map(rule => rule.empId)
    );

    const canPlaceShift_ = (empId, dateKey, candidateShiftCode) => {
      const byDate = plan.get(empId);
      if (!byDate) return true;
      const candidate = shiftStartEnd_(dateKey, candidateShiftCode);
      if (!candidate) return false;

      const prevDateKey = getDateKeyOffset(dateKey, -1);
      if (prevDateKey) {
        const prevCode = byDate.get(prevDateKey);
        if (!isOffCode_(prevCode)) {
          const prevShift = shiftStartEnd_(prevDateKey, prevCode);
          if (prevShift) {
            const hours = (candidate.start.getTime() - prevShift.end.getTime()) / 36e5;
            if (hours < candidate.minRestHours) return false;
          }
        }
      }

      const nextDateKey = getDateKeyOffset(dateKey, 1);
      if (nextDateKey) {
        const nextCode = byDate.get(nextDateKey);
        if (!isOffCode_(nextCode)) {
          const nextShift = shiftStartEnd_(nextDateKey, nextCode);
          if (nextShift) {
            const hours = (nextShift.start.getTime() - candidate.end.getTime()) / 36e5;
            if (hours < candidate.minRestHours) return false;
          }
        }
      }
      return true;
    };

    const restCountR = new Map(this.people.map(p => [p.empId, 0]));
    const restCountr = new Map(this.people.map(p => [p.empId, 0]));
    const targetr = saturdays.length;

    const assignRest = (empId, dateKey, code) => {
      const byDate = plan.get(empId);
      if (!byDate || byDate.get(dateKey) !== "") return false;
      byDate.set(dateKey, code);
      if (code === "R") {
        restCountR.set(empId, (restCountR.get(empId) || 0) + 1);
      } else if (code === "r") {
        restCountr.set(empId, (restCountr.get(empId) || 0) + 1);
      }
      return true;
    };

    const ensureNightKey = dateKey => {
      if (!nightKeySet.has(dateKey)) nightKeySet.add(dateKey);
    };

    // Phase 1: Leave + Fixed + availability warn
    for (const item of this.leave) {
      const dateKey = fmtDate(item.date);
      const byDate = plan.get(item.empId);
      if (byDate && byDate.has(dateKey)) {
        byDate.set(dateKey, item.leaveType);
      }
    }

    const availableByDate = new Map();
    for (const { dateKey } of days) {
      let available = 0;
      for (const person of this.people) {
        if (!this.leaveSet.has(`${person.empId}#${dateKey}`)) {
          available += 1;
        }
      }
      availableByDate.set(dateKey, available);
      if (available < 3) {
        Logger.log(`[WARN] date=${dateKey} availableWorkers<3; coverage may be impossible due to leave`);
      }
    }

    for (const rule of this.fixedRules) {
      const byDate = plan.get(rule.empId);
      if (!byDate) continue;
      const datesInRule = expandDateRange(rule.dateFrom, rule.dateTo);
      for (const date of datesInRule) {
        const dateKey = fmtDate(date);
        if (!byDate.has(dateKey)) continue;
        if (byDate.get(dateKey)) continue;
        byDate.set(dateKey, rule.shiftCode);
        if (isNightCode_(rule.shiftCode)) ensureNightKey(dateKey);
      }
    }

    // Phase 2-1: weekly R (exactly 1 per week)
    for (const person of this.people) {
      const byDate = plan.get(person.empId);
      if (!byDate) continue;
      for (const weekDates of weekBuckets) {
        const weekDateKeys = weekDates.map(d => fmtDate(d));
        const existingR = weekDateKeys.filter(dateKey => byDate.get(dateKey) === "R").length;
        if (existingR > 1) {
          Logger.log(`[WARN] multiple R already set for empId=${person.empId} week=${weekDateKeys[0]}`);
          continue;
        }
        if (existingR === 1) continue;
        const candidates = weekDateKeys.filter(dateKey => byDate.get(dateKey) === "");
        if (!candidates.length) {
          Logger.log(`[WARN] cannot place weekly R for empId=${person.empId} week=${weekDateKeys[0]}`);
          continue;
        }
        candidates.sort((a, b) => {
          const availDiff = (availableByDate.get(b) || 0) - (availableByDate.get(a) || 0);
          if (availDiff !== 0) return availDiff;
          const aDow = dow_(new Date(a));
          const bDow = dow_(new Date(b));
          const aWeekend = aDow === 0 || aDow === 6;
          const bWeekend = bDow === 0 || bDow === 6;
          if (aWeekend !== bWeekend) return aWeekend ? 1 : -1;
          return a.localeCompare(b);
        });
        assignRest(person.empId, candidates[0], "R");
      }
    }

    // Phase 2-2: monthly r quota
    for (const person of this.people) {
      const byDate = plan.get(person.empId);
      if (!byDate) continue;
      for (const date of saturdays) {
        if ((restCountr.get(person.empId) || 0) >= targetr) break;
        const dateKey = fmtDate(date);
        assignRest(person.empId, dateKey, "r");
      }
      for (const { dateKey } of days) {
        if ((restCountr.get(person.empId) || 0) >= targetr) break;
        if (byDate.get(dateKey) === "R") continue;
        assignRest(person.empId, dateKey, "r");
      }
      if ((restCountr.get(person.empId) || 0) < targetr) {
        Logger.log(`[WARN] cannot fill r quota for empId=${person.empId} target=${targetr} actual=${restCountr.get(person.empId) || 0}`);
      }
    }

    // Phase 2-3: 7-workday rest enforcement (use r)
    for (const person of this.people) {
      const byDate = plan.get(person.empId);
      if (!byDate) continue;
      let streak = 0;
      for (let i = 0; i < days.length; i += 1) {
        const dateKey = days[i].dateKey;
        const val = byDate.get(dateKey);
        const isNonWork = val === "R" || val === "r" || isLeaveCode_(val);
        if (isNonWork) {
          streak = 0;
          continue;
        }
        streak += 1;
        if (streak >= 7) {
          const windowStart = Math.max(0, i - 6);
          let placed = false;
          for (let j = windowStart; j <= i; j += 1) {
            const targetKey = days[j].dateKey;
            if (byDate.get(targetKey) === "") {
              if ((restCountr.get(person.empId) || 0) < targetr) {
                byDate.set(targetKey, "r");
                restCountr.set(person.empId, (restCountr.get(person.empId) || 0) + 1);
                placed = true;
                streak = 0;
                break;
              }
            }
          }
          if (!placed) {
            Logger.log(`[WARN] cannot enforce 7-workday rest for empId=${person.empId} window=${days[windowStart].dateKey}~${days[i].dateKey}`);
          }
        }
      }
    }

    const generalShiftPool = ["早", "午", "夜"].filter(code => shiftByCode.has(code));
    if (!generalShiftPool.length) {
      for (const def of Object.values(this.shiftDefs)) {
        if (String(def.isOff || "").toUpperCase() === "Y") continue;
        if (String(def.shiftCode || "").includes("常夜")) continue;
        if (!generalShiftPool.includes(def.shiftCode)) generalShiftPool.push(def.shiftCode);
      }
    }

    // Phase 3-1: daily coverage
    for (const { dateKey } of days) {
      let earlyCount = 0;
      let noonCount = 0;
      let nightCount = 0;
      for (const person of this.people) {
        const val = plan.get(person.empId).get(dateKey);
        if (isEarlyCode_(val)) earlyCount += 1;
        if (isNoonCode_(val)) noonCount += 1;
        if (isNightCode_(val)) nightCount += 1;
      }
      if (nightCount >= 1) ensureNightKey(dateKey);

      const emptyCandidates = () => this.people
        .map(p => p.empId)
        .filter(empId => plan.get(empId).get(dateKey) === "");

      const placeShift = (empId, shiftCode) => {
        if (!canPlaceShift_(empId, dateKey, shiftCode)) return false;
        plan.get(empId).set(dateKey, shiftCode);
        if (isNightCode_(shiftCode)) ensureNightKey(dateKey);
        return true;
      };

      if (earlyCount === 0) {
        const candidates = emptyCandidates();
        for (const empId of candidates) {
          if (placeShift(empId, "早")) {
            earlyCount += 1;
            break;
          }
        }
      }

      if (noonCount === 0) {
        const candidates = emptyCandidates();
        for (const empId of candidates) {
          if (placeShift(empId, "午")) {
            noonCount += 1;
            break;
          }
        }
      }

      if (nightCount === 0) {
        const candidates = emptyCandidates();
        let placed = false;
        for (const empId of candidates) {
          if (fixedNightEmpSet.has(empId) && placeShift(empId, "常夜")) {
            nightCount += 1;
            placed = true;
            break;
          }
        }
        if (!placed) {
          for (const empId of candidates) {
            if (placeShift(empId, "夜")) {
              nightCount += 1;
              break;
            }
          }
        }
      }
    }

    // Phase 3-2: fill remaining shifts
    for (const person of this.people) {
      const byDate = plan.get(person.empId);
      if (!byDate) continue;
      for (const { dateKey } of days) {
        if (byDate.get(dateKey)) continue;
        const currentNightCount = this.people.reduce((sum, p) => {
          const val = plan.get(p.empId).get(dateKey);
          return sum + (isNightCode_(val) ? 1 : 0);
        }, 0);
        const nightAllowed = currentNightCount === 0;
        let placed = false;

        if (fixedNightEmpSet.has(person.empId) && nightAllowed) {
          if (canPlaceShift_(person.empId, dateKey, "常夜")) {
            byDate.set(dateKey, "常夜");
            ensureNightKey(dateKey);
            placed = true;
          }
        }

        if (!placed) {
          const pool = generalShiftPool.filter(code => (code === "夜" ? nightAllowed : true));
          for (const code of pool) {
            if (canPlaceShift_(person.empId, dateKey, code)) {
              byDate.set(dateKey, code);
              if (isNightCode_(code)) ensureNightKey(dateKey);
              placed = true;
              break;
            }
          }
        }

        if (!placed) {
          if ((restCountr.get(person.empId) || 0) < targetr) {
            byDate.set(dateKey, "r");
            restCountr.set(person.empId, (restCountr.get(person.empId) || 0) + 1);
            placed = true;
          }
        }

        if (!placed) {
          Logger.log(`[WARN] no feasible shift due to minRest/nightCap empId=${person.empId} date=${dateKey}`);
        }
      }
    }

    const validatePlan = () => {
      for (const { dateKey } of days) {
        let earlyCount = 0;
        let noonCount = 0;
        let nightCount = 0;
        for (const person of this.people) {
          const val = plan.get(person.empId).get(dateKey);
          if (isEarlyCode_(val)) earlyCount += 1;
          if (isNoonCode_(val)) noonCount += 1;
          if (isNightCode_(val)) nightCount += 1;
        }
        if (earlyCount < 1 || noonCount < 1 || nightCount !== 1) {
          Logger.log(`[WARN] coverage unmet date=${dateKey} early=${earlyCount} noon=${noonCount} night=${nightCount}`);
        }
      }

      for (const person of this.people) {
        const byDate = plan.get(person.empId);
        if (!byDate) continue;
        for (const weekDates of weekBuckets) {
          const weekDateKeys = weekDates.map(d => fmtDate(d));
          const rCount = weekDateKeys.filter(dateKey => byDate.get(dateKey) === "R").length;
          if (rCount !== 1) {
            Logger.log(`[WARN] weekly R count mismatch empId=${person.empId} week=${weekDateKeys[0]} count=${rCount}`);
          }
        }
        const rTotal = days.reduce((sum, { dateKey }) => sum + (byDate.get(dateKey) === "r" ? 1 : 0), 0);
        if (rTotal !== targetr) {
          Logger.log(`[WARN] monthly r count mismatch empId=${person.empId} target=${targetr} actual=${rTotal}`);
        }

        let streak = 0;
        for (let i = 0; i < days.length; i += 1) {
          const dateKey = days[i].dateKey;
          const val = byDate.get(dateKey);
          const isNonWork = val === "R" || val === "r" || isLeaveCode_(val);
          if (isNonWork) {
            streak = 0;
          } else {
            streak += 1;
            if (streak >= 7) {
              Logger.log(`[WARN] 7+ workday streak empId=${person.empId} date=${dateKey}`);
            }
          }
        }

        for (const { dateKey } of days) {
          const code = byDate.get(dateKey);
          if (!code || isOffCode_(code)) continue;
          if (code === "常夜" && !fixedNightEmpSet.has(person.empId)) {
            Logger.log(`[WARN] non-fixed night assigned empId=${person.empId} date=${dateKey}`);
          }
          if (!canPlaceShift_(person.empId, dateKey, code)) {
            Logger.log(`[WARN] min rest violation empId=${person.empId} date=${dateKey}`);
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
