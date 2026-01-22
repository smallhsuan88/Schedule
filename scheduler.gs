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

    // 在任何情況下，不得為了塞滿 R/r 配額而讓某天 off 人數 > 2 或造成當天缺早/午/夜；若配額與 coverage 衝突，優先維持 daily coverage，並將無法安置的 R/r 以 warning 回報。
    const shiftByCode = new Map(Object.values(this.shiftDefs).map(def => [def.shiftCode, def]));
    const FIXED_NIGHT_EMP_ID = "T00128";
    const hasFixedNightRule = this.fixedRules.some(rule => rule.empId === FIXED_NIGHT_EMP_ID);
    const fixedNightEmpSet = new Set(hasFixedNightRule ? [FIXED_NIGHT_EMP_ID] : []);
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

    const dayAssigned = {};
    const nightTaken = {};
    for (const { dateKey } of days) {
      dayAssigned[dateKey] = { early: 0, noon: 0, night: 0 };
      nightTaken[dateKey] = false;
    }

    const minRestOk_ = (prevDateKey, prevCode, curDateKey, curCode, minHours = 11) => {
      if (!isWorkCode(prevCode) || !isWorkCode(curCode)) return true;
      const prevShift = shiftStartEnd_(prevDateKey, prevCode);
      const curShift = shiftStartEnd_(curDateKey, curCode);
      if (!prevShift || !curShift) return false;
      const hours = (curShift.start.getTime() - prevShift.end.getTime()) / 36e5;
      return hours >= minHours;
    };

    const canPlaceShift = (empId, dateKey, candidateShiftCode, options = {}) => {
      const byDate = plan.get(empId);
      if (!byDate) return false;
      if (byDate.get(dateKey) !== "") return false;
      if (!shiftByCode.has(candidateShiftCode)) return false;
      if (hasFixedNightRule && empId === FIXED_NIGHT_EMP_ID && isWorkCode(candidateShiftCode) && candidateShiftCode !== "常夜") {
        return false;
      }
      if (String(candidateShiftCode || "").includes("常夜") && (!hasFixedNightRule || empId !== FIXED_NIGHT_EMP_ID)) {
        return false;
      }
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

    const countOffPeople = dateKey => {
      let count = 0;
      for (const person of this.people) {
        const code = plan.get(person.empId).get(dateKey);
        if (isOffCode(code)) count += 1;
      }
      return count;
    };

    const pickCandidateForShift = (dateKey, shiftCode, excludeEmpIds = new Set()) => {
      for (const person of this.people) {
        if (excludeEmpIds.has(person.empId)) continue;
        const byDate = plan.get(person.empId);
        if (!byDate || byDate.get(dateKey) !== "") continue;
        if (!canPlaceShift(person.empId, dateKey, shiftCode)) continue;
        return person.empId;
      }
      return null;
    };

    const restCountR = new Map(this.people.map(p => [p.empId, 0]));
    const restCountr = new Map(this.people.map(p => [p.empId, 0]));

    const assignRest = (empId, idx, code) => {
      const dateKey = dateKeys[idx];
      const byDate = plan.get(empId);
      if (!byDate || byDate.get(dateKey) !== "") return false;
      if (this.leaveSet.has(`${empId}#${dateKey}`)) return false;
      if (countOffPeople(dateKey) >= 2) return false;
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
      if (countOffPeople(toKey) >= 2) return false;
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
      if (!canPlaceShift(empId, dateKey, shiftCode)) return false;
      if (isNightCode_(shiftCode) && nightTaken[dateKey]) return false;
      plan.get(empId).set(dateKey, shiftCode);
      markAssigned(dateKey, shiftCode);
      return true;
    };

    // Phase A: Leave (do not overwrite)
    for (const item of this.leave) {
      const dateKey = fmtDate(item.date);
      const byDate = plan.get(item.empId);
      if (byDate && byDate.has(dateKey)) {
        byDate.set(dateKey, item.leaveType);
      }
    }

    for (const { dateKey } of days) {
      let leaveCount = 0;
      for (const person of this.people) {
        const code = plan.get(person.empId).get(dateKey);
        if (isLeaveCode_(code)) leaveCount += 1;
      }
      const availableWorkers = this.people.length - leaveCount;
      if (availableWorkers < 3) {
        Logger.log(`[WARN] coverage impossible date=${dateKey} leaveCount=${leaveCount}`);
      }
    }

    // Phase B: lock daily coverage slots (early/noon/night)
    for (const { dateKey } of days) {
      let nightLocked = false;
      if (hasFixedNightRule && fixedNightEmpSet.has(FIXED_NIGHT_EMP_ID)) {
        const byDate = plan.get(FIXED_NIGHT_EMP_ID);
        if (byDate && byDate.get(dateKey) === "") {
          if (shiftByCode.has("常夜") && placeShift(FIXED_NIGHT_EMP_ID, dateKey, "常夜")) {
            nightLocked = true;
          } else {
            Logger.log(`[WARN] coverage lock failed date=${dateKey} missing=night`);
          }
        }
      }

      if (!nightLocked) {
        if (shiftByCode.has(coverageShiftCodes.night)) {
          const nightCandidate = pickCandidateForShift(
            dateKey,
            coverageShiftCodes.night,
            new Set(fixedNightEmpSet)
          );
          if (nightCandidate) {
            placeShift(nightCandidate, dateKey, coverageShiftCodes.night);
            nightLocked = true;
          } else {
            Logger.log(`[WARN] coverage lock failed date=${dateKey} missing=night`);
          }
        } else {
          Logger.log(`[WARN] coverage lock failed date=${dateKey} missing=night`);
        }
      }

      if (shiftByCode.has(coverageShiftCodes.early)) {
        const earlyCandidate = pickCandidateForShift(dateKey, coverageShiftCodes.early);
        if (earlyCandidate) {
          placeShift(earlyCandidate, dateKey, coverageShiftCodes.early);
        } else {
          Logger.log(`[WARN] coverage lock failed date=${dateKey} missing=early`);
        }
      } else {
        Logger.log(`[WARN] coverage lock failed date=${dateKey} missing=early`);
      }

      if (shiftByCode.has(coverageShiftCodes.noon)) {
        const noonCandidate = pickCandidateForShift(dateKey, coverageShiftCodes.noon);
        if (noonCandidate) {
          placeShift(noonCandidate, dateKey, coverageShiftCodes.noon);
        } else {
          Logger.log(`[WARN] coverage lock failed date=${dateKey} missing=noon`);
        }
      } else {
        Logger.log(`[WARN] coverage lock failed date=${dateKey} missing=noon`);
      }
    }

    // Phase C1: place monthly R/r quotas (only on unassigned, non-leave slots)
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
      const missingR = monthSundayCount - (restCountR.get(person.empId) || 0);
      const missingr = monthSaturdayCount - (restCountr.get(person.empId) || 0);
      if (missingR > 0 || missingr > 0) {
        Logger.log(`[WARN] cannot place monthly R/r quota empId=${person.empId} missingR=${Math.max(0, missingR)} missingr=${Math.max(0, missingr)}`);
      }
    }

    // Phase C2: weekly R (at least 1 per week)
    for (const person of this.people) {
      const byDate = plan.get(person.empId);
      if (!byDate) continue;
      for (const bucket of weekBucketsIdx) {
        const hasR = bucket.some(idx => byDate.get(dateKeys[idx]) === "R");
        if (hasR) continue;
        let placed = false;
        if ((restCountR.get(person.empId) || 0) < monthSundayCount) {
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
        let targetIdx = null;
        for (let idx = startIdx; idx <= endIdx; idx += 1) {
          if (plan.get(person.empId).get(dateKeys[idx]) === "" && countOffPeople(dateKeys[idx]) < 2) {
            targetIdx = idx;
            break;
          }
        }
        if (targetIdx === null) {
          Logger.log(`[WARN] cannot enforce 7-day rest window empId=${person.empId} range=${dateKeys[startIdx]}~${dateKeys[endIdx]}`);
          continue;
        }

        if ((restCountr.get(person.empId) || 0) < monthSaturdayCount) {
          if (!assignRest(person.empId, targetIdx, "r")) {
            Logger.log(`[WARN] cannot place r for 7-day rest window empId=${person.empId} range=${dateKeys[startIdx]}~${dateKeys[endIdx]}`);
          }
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
        if (!moved && (restCountR.get(person.empId) || 0) < monthSundayCount) {
          if (assignRest(person.empId, targetIdx, "R")) moved = true;
        }
        if (!moved) {
          Logger.log(`[WARN] cannot move rest into 7-day rest window empId=${person.empId} range=${dateKeys[startIdx]}~${dateKeys[endIdx]}`);
        }
      }
    }

    // Phase D: fill remaining shifts (no night beyond night-cap)
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
          if (shiftByCode.has(coverageShiftCodes.early) && placeShift(person.empId, dateKey, coverageShiftCodes.early)) {
            placed = true;
          } else if (shiftByCode.has(coverageShiftCodes.noon) && placeShift(person.empId, dateKey, coverageShiftCodes.noon)) {
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
        let offCount = 0;
        for (const person of this.people) {
          const code = plan.get(person.empId).get(dateKey);
          if (isOffCode(code)) offCount += 1;
          if (isEarlyCode_(code)) earlyCount += 1;
          if (isNoonCode_(code)) noonCount += 1;
          if (isNightCode_(code)) nightCount += 1;
        }
        if (earlyCount < 1 || noonCount < 1 || nightCount !== 1) {
          Logger.log(`[VIOLATION] date=${dateKey} type=daily_coverage details=early=${earlyCount} noon=${noonCount} night=${nightCount}`);
        }
        if (offCount > 2) {
          Logger.log(`[VIOLATION] date=${dateKey} type=daily_off_cap details=offCount=${offCount}`);
        }
      }

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

        if (hasFixedNightRule && person.empId === FIXED_NIGHT_EMP_ID) {
          for (const { dateKey } of days) {
            const code = byDate.get(dateKey);
            if (isWorkCode(code) && code !== "常夜") {
              Logger.log(`[VIOLATION] empId=${person.empId} type=fixed_night details=date=${dateKey} code=${code}`);
            }
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
