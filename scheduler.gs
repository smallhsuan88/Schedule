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

    const plan = new Map();
    for (const person of this.people) {
      const byDate = new Map();
      for (const { dateKey } of days) {
        byDate.set(dateKey, "");
      }
      plan.set(person.empId, byDate);
    }

    for (const item of this.leave) {
      const dateKey = fmtDate(item.date);
      const byDate = plan.get(item.empId);
      if (byDate && byDate.has(dateKey)) {
        byDate.set(dateKey, item.leaveType);
      }
    }

    for (const rule of this.fixedRules) {
      const byDate = plan.get(rule.empId);
      if (!byDate) continue;
      const dates = expandDateRange(rule.dateFrom, rule.dateTo);
      for (const date of dates) {
        const dateKey = fmtDate(date);
        if (!byDate.has(dateKey)) continue;
        if (byDate.get(dateKey)) continue;
        byDate.set(dateKey, rule.shiftCode);
      }
    }

    const monthStart = new Date(year, monthIndex, 1);
    const weekBuckets = new Map();
    for (const { date } of days) {
      const idx = weekIndex_(date, monthStart);
      if (!weekBuckets.has(idx)) weekBuckets.set(idx, []);
      weekBuckets.get(idx).push(date);
    }

    const sundays = days.filter(({ date }) => dow_(date) === 0);
    const saturdays = days.filter(({ date }) => dow_(date) === 6);
    const targetR = sundays.length;
    const targetr = saturdays.length;

    const restCountR = new Map(this.people.map(p => [p.empId, 0]));
    const restCountr = new Map(this.people.map(p => [p.empId, 0]));
    for (const person of this.people) {
      const byDate = plan.get(person.empId);
      if (!byDate) continue;
      for (const { dateKey } of days) {
        const val = byDate.get(dateKey);
        if (val === "R") restCountR.set(person.empId, (restCountR.get(person.empId) || 0) + 1);
        if (val === "r") restCountr.set(person.empId, (restCountr.get(person.empId) || 0) + 1);
      }
    }

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

    const assignRestDaysPerEmployee = () => {
      for (const person of this.people) {
        const byDate = plan.get(person.empId);
        if (!byDate) continue;

        for (const { dateKey } of sundays) {
          assignRest(person.empId, dateKey, "R");
        }

        for (const { dateKey } of saturdays) {
          assignRest(person.empId, dateKey, "r");
        }

        const fillQuota = (code, target) => {
          const count = code === "R" ? restCountR.get(person.empId) || 0 : restCountr.get(person.empId) || 0;
          if (count >= target) return;
          for (const { dateKey } of days) {
            if (code === "R" && (restCountR.get(person.empId) || 0) >= target) break;
            if (code === "r" && (restCountr.get(person.empId) || 0) >= target) break;
            assignRest(person.empId, dateKey, code);
          }
          const afterCount = code === "R" ? restCountR.get(person.empId) || 0 : restCountr.get(person.empId) || 0;
          if (afterCount < target) {
            Logger.log(`[WARN] cannot fill ${code} quota for empId=${person.empId} target=${target} actual=${afterCount}`);
          }
        };

        fillQuota("R", targetR);
        fillQuota("r", targetr);

        for (const [weekIdx, weekDates] of weekBuckets.entries()) {
          const weekDateKeys = weekDates.map(d => fmtDate(d));
          const hasR = weekDateKeys.some(dateKey => byDate.get(dateKey) === "R");
          const hasRest = weekDateKeys.some(dateKey => {
            const val = byDate.get(dateKey);
            return val === "R" || val === "r";
          });

          let currentHasRest = hasRest;
          if (!hasR) {
            const sundayKey = weekDates.find(d => dow_(d) === 0);
            const preferredKey = sundayKey ? fmtDate(sundayKey) : null;
            const candidates = weekDateKeys.filter(dateKey => byDate.get(dateKey) === "");
            const pickKey = (preferredKey && byDate.get(preferredKey) === "") ? preferredKey : candidates[0];
            if (pickKey) {
              assignRest(person.empId, pickKey, "R");
              currentHasRest = true;
            } else {
              Logger.log(`[WARN] cannot place weekly R for empId=${person.empId} week=${weekIdx}`);
            }
          }

          if (!currentHasRest) {
            const candidates = weekDateKeys.filter(dateKey => byDate.get(dateKey) === "");
            const pickKey = candidates[0];
            if (pickKey) {
              const remainingr = targetr - (restCountr.get(person.empId) || 0);
              const remainingR = targetR - (restCountR.get(person.empId) || 0);
              const code = remainingr > 0 ? "r" : remainingR > 0 ? "R" : "R";
              assignRest(person.empId, pickKey, code);
            } else {
              Logger.log(`[WARN] cannot place weekly rest for empId=${person.empId} week=${weekIdx}`);
            }
          }
        }
      }
    };

    assignRestDaysPerEmployee();

    const shiftCodes = Object.values(this.shiftDefs)
      .filter(def => String(def.isOff || "").toUpperCase() !== "Y")
      .map(def => def.shiftCode)
      .filter(code => code && !String(code).includes("常夜"));

    for (const person of this.people) {
      const byDate = plan.get(person.empId);
      if (!byDate) continue;
      for (const { dateKey } of days) {
        if (byDate.get(dateKey)) continue;
        const available = shiftCodes.filter(code => canPlaceShift_(person.empId, dateKey, code));
        if (available.length) {
          const picked = available[Math.floor(Math.random() * available.length)];
          byDate.set(dateKey, picked);
        } else {
          const dateObj = new Date(dateKey);
          const fallback = dow_(dateObj) === 6 ? "r" : "R";
          byDate.set(dateKey, fallback);
          Logger.log(`[WARN] no shift meets min rest for empId=${person.empId} date=${dateKey}, fallback=${fallback}`);
        }
      }
    }

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
