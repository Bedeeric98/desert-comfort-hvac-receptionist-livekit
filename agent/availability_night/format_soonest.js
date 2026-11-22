const ORDINAL_WORDS = {
  1: "first", 2: "second", 3: "third", 4: "fourth", 5: "fifth",
  6: "sixth", 7: "seventh", 8: "eighth", 9: "ninth", 10: "tenth",
  11: "eleventh", 12: "twelfth", 13: "thirteenth", 14: "fourteenth", 15: "fifteenth",
  16: "sixteenth", 17: "seventeenth", 18: "eighteenth", 19: "nineteenth", 20: "twentieth",
  21: "twenty-first", 22: "twenty-second", 23: "twenty-third", 24: "twenty-fourth", 25: "twenty-fifth",
  26: "twenty-sixth", 27: "twenty-seventh", 28: "twenty-eighth", 29: "twenty-ninth", 30: "thirtieth",
  31: "thirty-first",
};
const ORDINAL = (n) => ORDINAL_WORDS[n] || String(n);

const NUMBER_WORDS = {
  0: "zero", 1: "one", 2: "two", 3: "three", 4: "four", 5: "five",
  6: "six", 7: "seven", 8: "eight", 9: "nine", 10: "ten",
  11: "eleven", 12: "twelve", 13: "thirteen", 14: "fourteen", 15: "fifteen",
  16: "sixteen", 17: "seventeen", 18: "eighteen", 19: "nineteen",
  20: "twenty", 30: "thirty", 40: "forty", 50: "fifty"
};

function minuteWord(min) {
  if (min < 10) return `oh-${NUMBER_WORDS[min]}`;
  if (NUMBER_WORDS[min]) return NUMBER_WORDS[min];
  const tens = Math.floor(min / 10) * 10;
  const ones = min % 10;
  return `${NUMBER_WORDS[tens]}-${NUMBER_WORDS[ones]}`;
}

function speakTime(isoString) {
  const d = new Date(isoString);
  if (isNaN(d.getTime())) return null;
  const opts = { timeZone: "America/Phoenix", hour: "numeric", minute: "numeric", hour12: true };
  const parts = new Intl.DateTimeFormat("en-US", opts).formatToParts(d);
  const hourNum = parseInt(parts.find(p => p.type === "hour").value, 10);
  const minute = parseInt(parts.find(p => p.type === "minute").value, 10);
  const dayPeriod = parts.find(p => p.type === "dayPeriod").value.toLowerCase();
  const hourWord = NUMBER_WORDS[hourNum] || String(hourNum);

  if (minute === 0) return `${hourWord} ${dayPeriod}`;
  return `${hourWord}-${minuteWord(minute)} ${dayPeriod}`;
}

function speakOrdinalDateLabel(dateKey, todayKey, tomorrowKey) {
  const d = new Date(dateKey + "T12:00:00-07:00");
  if (isNaN(d.getTime())) return null;
  const weekday = new Intl.DateTimeFormat("en-US", { timeZone: "America/Phoenix", weekday: "long" }).format(d);
  const dayNum = parseInt(new Intl.DateTimeFormat("en-US", { timeZone: "America/Phoenix", day: "numeric" }).format(d), 10);
  const dateLabel = `${weekday} the ${ORDINAL(dayNum)}`;

  if (dateKey === todayKey) return { prefix: "today being", dateLabel };
  if (dateKey === tomorrowKey) return { prefix: "tomorrow being", dateLabel };
  return { prefix: null, dateLabel };
}

function buildSoonestText(data, night) {
  const DATE_KEY_RE = /^\d{4}-\d{2}-\d{2}$/;

  const nowMST = new Date(new Date().toLocaleString("en-US", { timeZone: "America/Phoenix" }));
  const todayKey = nowMST.toISOString().slice(0, 10);
  const tomorrow = new Date(nowMST);
  tomorrow.setDate(tomorrow.getDate() + 1);
  const tomorrowKey = tomorrow.toISOString().slice(0, 10);

  const sortedDateKeys = Object.keys(data).filter(k => DATE_KEY_RE.test(k)).sort();

  const allSlots = [];
  for (const dateKey of sortedDateKeys) {
    const rawSlots = data[dateKey]?.slots;
    if (!Array.isArray(rawSlots)) continue;
    const uniqueSorted = [...new Set(rawSlots)].sort();
    for (const iso of uniqueSorted) {
      allSlots.push({ dateKey, iso });
    }
  }

  // After-hours calls: ONE finished sentence with only the first open time, no choice. The tool says it; the prompt only speaks it.
  if (night) {
    const first = allSlots[0];
    if (!first) return "";
    const label = speakOrdinalDateLabel(first.dateKey, todayKey, tomorrowKey);
    const time = speakTime(first.iso);
    if (label === null || time === null) return "";
    const when = label.prefix ? `${label.prefix.replace(" being", "")}, ${label.dateLabel},` : label.dateLabel;
    return `__NIGHT_LEAD__ ${when} at ${time}, and I'll book it for you right now - does that work?`;
  }

  const picked = allSlots.slice(0, 2);
  if (picked.length === 0) return "";

  const byDate = [];
  for (const slot of picked) {
    let group = byDate.find((g) => g.dateKey === slot.dateKey);
    if (!group) {
      group = { dateKey: slot.dateKey, times: [] };
      byDate.push(group);
    }
    group.times.push(slot.iso);
  }

  const clauses = [];
  for (const group of byDate) {
    const label = speakOrdinalDateLabel(group.dateKey, todayKey, tomorrowKey);
    if (label === null) continue;
    const spokenTimes = group.times.map(speakTime).filter((t) => t !== null);
    if (spokenTimes.length === 0) continue;

    const prefixPart = label.prefix ? `${label.prefix} ${label.dateLabel}` : label.dateLabel;
    if (spokenTimes.length === 1) {
      clauses.push(`${prefixPart} at ${spokenTimes[0]}`);
    } else {
      clauses.push(`${prefixPart} at ${spokenTimes[0]} Arizona time and also at ${spokenTimes[1]}`);
    }
  }

  if (clauses.length === 0) return "";

  let inner;
  if (clauses.length === 1) {
    inner = clauses[0];
  } else {
    inner = `${clauses[0]} Arizona time and ${clauses[1]}`;
  }
  return `We have availability ${inner}. When works for you?`;
}

// hours_mode comes from the agent's own call-start clock check (code, never the model). Anything but exactly "night" keeps the day answer.
let night = false;
try {
  night = $("Check Availability and Book Desert Comfort LiveKit").first().json.body.hours_mode === "night";
} catch (e) {
  night = false;
}

let text = "";
try {
  const data = $("__SLOTS_NODE__").first().json;
  if (data && typeof data === "object" && !Array.isArray(data)) {
    text = buildSoonestText(data, night);
  }
} catch (e) {
  text = "";
}

return [{ json: { text } }];
