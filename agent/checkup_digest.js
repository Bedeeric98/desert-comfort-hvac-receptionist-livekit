// Build Digest - the exact code that runs in the "Desert Comfort Call Check-Up Daily Digest" workflow's Code node.
// Reads the check-up rows (aggregated by the node before it) and the State Writer ledger, covers the last 24 hours, and returns the Slack
// text, the email text and the subject. Never throws: a bad row is skipped, and the digest still goes out (it is also a heartbeat that says
// the check-up pipeline is alive).
// Format (rewritten 2026-10-04 at Admin's request): plain-language rule names instead of rule codes, one block per flagged call, a blank line
// between every section, and a "what to do" line for every red flag. Slack gets *bold*, email gets plain text. Same inputs, same output keys.
const LEARNING_UNTIL = '2026-10-16'; // learning mode (set 2026-10-02, two weeks): yellow flags are digest-only; review the rules on this date
const HOURS = 24;

const rowsIn = (() => { try { return (($('Aggregate Check-Up Rows').first().json) || {}).rows || []; } catch (e) { return []; } })();
const ledgerIn = $input.all().map((i) => i.json).filter((r) => r && r.event_id);

const now = Date.now();
const since = now - HOURS * 3600 * 1000;
const when = (r) => Date.parse(r && r.occurred_at);
const inWindow = (r) => { const t = when(r); return !Number.isNaN(t) && t >= since && t <= now + 60000; };
const rows = rowsIn.filter((r) => r && r.job_id && inWindow(r)).sort((a, b) => when(a) - when(b));
const calls = ledgerIn.filter((r) => r.source === 'end_of_call' && inWindow(r)).length;

// One entry per rule: label = what happened, todo = what Admin should do about a RED one. Unknown rules fall back to a tidied rule name.
const RULES = {
  A1_said_booked_not_booked: { label: 'Agent told the caller they were booked, but no booking was made', todo: 'Check the GHL calendar. If there is no appointment, call the customer and book them.' },
  A2_agreed_never_booked: { label: 'Caller agreed to a time, but no booking was made', todo: 'Check the GHL calendar. If there is no appointment, call the customer and book them.' },
  A2_accepted_then_retracted: { label: 'Caller agreed to a time, then took it back' },
  A3_tool_failed: { label: 'A tool failed and never recovered', todo: 'Open the n8n execution and the agent log for this call to see which step failed.' },
  A3_tool_failed_then_recovered: { label: 'A tool failed once, then worked' },
  A3_tool_slow: { label: 'A tool was slow to answer' },
  A4_booked_but_recorded_wrong: { label: 'The booking and the recorded call result do not match', todo: 'Check the contact stage and the appointment in GHL and correct whichever is wrong.' },
  B1_wrong_tier_for_temperature: { label: 'The caller was given the wrong fee tier for the temperature they gave', todo: 'Listen to the recording. The customer may have been quoted the wrong fee.' },
  B2_emergency_missed: { label: 'The caller described an emergency but was treated as standard', todo: 'Listen to the recording and call the customer if they still need urgent help.' },
  B3_address_made_up: { label: 'The booked address does not match anything the caller said', todo: 'Confirm the address with the customer before the technician goes out.' },
  B4_optout_not_recorded: { label: 'The caller asked not to be contacted, but it was not recorded', todo: 'Mark this contact as do-not-contact in GHL now.' },
  B5_qualifying_question_skipped: { label: 'The fee was quoted before every qualifying question was asked', todo: 'Listen to the recording. A skipped question, like the temperature, can mean the customer was quoted the wrong fee.' },
  C1_caller_repeated_themselves: { label: 'Caller had to repeat themselves' },
  C2_agent_reasked: { label: 'Agent asked something the caller had already said' },
  C3_caller_corrected_agent: { label: 'Caller corrected the agent' },
  C4_address_asked_3_times: { label: 'Agent asked for the address three or more times' },
  C5_caller_did_not_understand: { label: 'Caller did not understand the agent' },
  C6_temperature_heard_differently: { label: 'The transcript and the agent heard different temperatures' },
  C7_optout_then_kept_talking: { label: 'Caller opted out, then tried to take it back or kept talking, and the take-back was not recorded' },
  D1_caller_said_hello: { label: 'Caller said "hello" or "are you there" (the agent went quiet)' },
  D2_agent_stalled: { label: 'Agent used a stalling line more than once' },
  D3_session_errors: { label: 'The call session logged errors' },
  D4_said_checking_but_nothing_ran: { label: 'Agent said it was checking the schedule, but the check only ran much later' },
  E1_walked_away_after_quote: { label: 'Caller walked away after hearing the fee' },
  E2_long_call_no_outcome: { label: 'Long call that ended with no outcome' },
  CHECKUP_ERROR: { label: 'The automatic check-up itself failed on this call', todo: 'Review this call by hand. The automatic check did not run.' },
};
const tidy = (id) => String(id || '').replace(/^[A-E]\d+_?/, '').replace(/_/g, ' ').trim() || String(id || 'unknown rule');
const ruleLabel = (id) => (RULES[id] && RULES[id].label) || tidy(id);
const ruleTodo = (id) => (RULES[id] && RULES[id].todo) || '';

const OUTCOMES = {
  booked_standard: 'Booked (standard)', booked_emergency: 'Booked (emergency)', qualified_not_booked: 'Qualified, but not booked',
  transferred: 'Transferred to a person', out_of_area: 'Outside the service area', do_not_call: 'Asked not to be contacted',
  wrong_person: 'Wrong person', no_meaningful_conversation: 'No meaningful conversation', dead_air: 'Dead air (no caller speech)',
};
const outcomeLabel = (o) => OUTCOMES[o] || (o ? String(o).replace(/_/g, ' ') : 'unknown');

// What the call result SHOULD have been, worked out from the flags. The agent has a Python copy of this logic and of RULES / OUTCOMES in
// call_messages.py (it builds the immediate alert). Keep RULES and OUTCOMES identical in both copies.
const correctResult = (r, flags) => {
  const serious = (flags || []).filter((f) => f && (f.severity === 'RED' || f.severity === 'ERROR'));
  const ids = serious.length ? serious.map((f) => String(f.rule || '')) : String(r.red_rules || '').split(',').filter(Boolean);
  const has = (p) => ids.some((id) => id.startsWith(p));
  const evidence = (p) => { const f = serious.find((x) => String(x.rule || '').startsWith(p)); return f ? String(f.evidence || '') : ''; };
  const temp = parseFloat(r.caller_temperature);
  const wasBooked = r.outcome === 'booked_standard' || r.outcome === 'booked_emergency';
  const tier = r.outcome === 'booked_emergency' ? 'emergency' : (r.outcome === 'booked_standard' ? 'standard' : (r.quote_tier === 'emergency' || r.quote_tier === 'standard' ? r.quote_tier : ''));
  const booked = (t) => (t ? `Booked (${t})` : 'Booked');
  if (r.severity === 'ERROR' || has('CHECKUP_ERROR')) return 'Unknown - the automatic check did not run';
  if (has('B4_')) return 'Asked not to be contacted - this contact should be marked do-not-contact';
  if (has('A4_')) return /^a booking succeeded/i.test(evidence('A4_')) ? `${booked(tier)} - a booking did succeed` : 'Qualified, but not booked - no booking actually succeeded';
  if (has('B1_') || has('B2_')) {
    const want = has('B2_') ? 'emergency' : (Number.isFinite(temp) ? (temp >= 90 ? 'emergency' : 'standard') : '');
    return `${wasBooked ? 'Booked' : 'Qualified, but not booked'}${want ? ` (${want})` : ''} - ${want ? `handled at the ${want} tier` : 'handled at the right fee tier'}`;
  }
  if (has('A1_') || has('A2_')) return `${booked(tier)} - the customer was told a time was set, so an appointment should exist`;
  if (ids.includes('A3_tool_failed')) {
    return /book_appointment|check_availability/.test(evidence('A3_tool_failed'))
      ? `${booked(tier)} - the tool that books the customer failed`
      : `Same as recorded (${outcomeLabel(r.outcome)}) - the failed step did not change the result`;
  }
  if (has('B3_')) return 'Booked, with the address the customer actually gave';
  return 'Not certain - listen to the call';
};

const dateOpts = { timeZone: 'America/Phoenix', weekday: 'short', month: 'short', day: 'numeric' };
const timeOpts = { timeZone: 'America/Phoenix', hour: 'numeric', minute: '2-digit' };
const phoenix = (t) => `${new Date(t).toLocaleDateString('en-US', dateOpts)} at ${new Date(t).toLocaleTimeString('en-US', timeOpts)}`;
const parseFlags = (r) => { try { const f = JSON.parse(r.flags_json || '[]'); return Array.isArray(f) ? f : []; } catch (e) { return []; } };
// Shorten at a word boundary, never mid-word.
const clip = (s, n) => {
  const t = String(s || '').replace(/\s+/g, ' ').trim();
  if (t.length <= n) return t;
  const cut = t.slice(0, n);
  const sp = cut.lastIndexOf(' ');
  return (sp > n * 0.6 ? cut.slice(0, sp) : cut).replace(/[\s,;:.]+$/, '') + '...';
};
const plural = (n, one, many) => `${n} ${n === 1 ? one : (many || one + 's')}`;

const red = rows.filter((r) => r.severity === 'RED');
const err = rows.filter((r) => r.severity === 'ERROR');
const yellow = rows.filter((r) => r.severity === 'YELLOW');

// Yellow rules: how many calls each one hit, plus one example from the flag detail so Admin can judge noise vs useful during learning mode.
const yellowCounts = {};
const yellowExample = {};
const walked = { standard: 0, emergency: 0, unknown: 0 };
for (const r of rows) {
  for (const rule of String(r.yellow_rules || '').split(',').filter(Boolean)) {
    yellowCounts[rule] = (yellowCounts[rule] || 0) + 1;
    if (rule.startsWith('E1_')) walked[r.quote_tier === 'standard' ? 'standard' : (r.quote_tier === 'emergency' ? 'emergency' : 'unknown')] += 1;
  }
  for (const f of parseFlags(r)) {
    if (f && f.severity === 'YELLOW' && f.rule && f.evidence && !yellowExample[f.rule]) yellowExample[f.rule] = f.evidence;
  }
}
const walkedTotal = walked.standard + walked.emergency + walked.unknown;
const learning = new Date().toISOString().slice(0, 10) <= LEARNING_UNTIL;

const build = (mode) => {
  const slack = mode === 'slack';
  const B = (s) => (slack ? `*${s}*` : s);
  const L_ = (s) => (slack ? `*${s}:*` : `${s}:`); // a label, colon inside the bold (house style = the Desert Comfort Telnyx canvas)
  const H = (s) => (slack ? `*${s}*` : s.toUpperCase());
  // Evidence contains words the caller and the agent actually said. In Slack, & < > start links and mentions, so they are escaped.
  const E = (s) => (slack ? String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;') : String(s));
  const dot = slack ? '•' : '-';
  const line = slack ? '━━━━━━━━━━━━━━━━━━' : '------------------------------';
  const L = [];

  L.push(H('At a glance'));
  L.push(`${dot} Calls handled: ${calls}`);
  L.push(`${dot} Calls flagged: ${rows.length}${rows.length ? ` (red ${red.length}, yellow ${yellow.length}${err.length ? `, check-up failures ${err.length}` : ''})` : ''}`);
  if (!rows.length) L.push('', 'All quiet: nothing was flagged in the last 24 hours.');

  if (red.length || err.length) {
    L.push('', line, '', `${H('Needs a look')} (you were already alerted at the time)`);
    [...red, ...err].forEach((r, i) => {
      const flags = parseFlags(r).filter((f) => f && (f.severity === 'RED' || f.severity === 'ERROR'));
      L.push('', B(`${i + 1}. ${phoenix(when(r))} - caller ending ${r.phone_last4 || '????'}`));
      L.push(`${L_('Call result')} ${outcomeLabel(r.outcome)}`);
      L.push(`${L_('What the correct result should be')} ${correctResult(r, parseFlags(r))}`);
      if (flags.length) {
        const todos = [];
        for (const f of flags) {
          L.push('', `${dot} ${B(ruleLabel(f.rule))}`);
          if (f.evidence) L.push(`  Detail: ${E(clip(f.evidence, 240))}`);
          const todo = ruleTodo(f.rule);
          if (todo && !todos.includes(todo)) todos.push(todo);
        }
        if (todos.length) L.push('', `${L_('Action Required')} ${todos.join(' ')}`);
      } else {
        const ids = String(r.red_rules || '').split(',').filter(Boolean);
        L.push('', `${dot} ${ids.length ? ids.map(ruleLabel).join('; ') : `Flagged ${r.severity}`}`);
      }
    });
  }

  // "Walked away after the fee quote" has its own section below, so it is left out of the minor notes (no double counting on the page).
  const minor = Object.entries(yellowCounts).filter(([rule]) => !rule.startsWith('E1_')).sort((a, b) => b[1] - a[1]);
  if (minor.length) {
    L.push('', line, '', `${H('Minor notes')} (counted, not alerted)`);
    for (const [rule, n] of minor) {
      L.push('', `${dot} ${B(ruleLabel(rule))}: ${plural(n, 'call')}`);
      if (yellowExample[rule]) L.push(`  Example: ${E(clip(yellowExample[rule], 160))}`);
    }
  }

  if (walkedTotal) {
    L.push('', line, '', `${H('Walked away after the fee quote')}: ${plural(walkedTotal, 'call')} (standard ${walked.standard}, emergency ${walked.emergency}${walked.unknown ? `, unknown ${walked.unknown}` : ''})`);
    const e1 = Object.keys(yellowExample).find((k) => k.startsWith('E1_'));
    if (e1) L.push(`  Example: ${E(clip(yellowExample[e1], 160))}`);
  }

  L.push('', line, '', learning
    ? `Learning mode until ${LEARNING_UNTIL}: only red flags alert at once. Yellow notes appear in this digest only. Say which notes were noise and which were useful, then review.`
    : `Learning mode ended ${LEARNING_UNTIL}: review which yellow notes should now alert at once.`);
  L.push('', 'Times are Phoenix time. Callers are shown by the last 4 digits of their number only.');
  return L.join('\n');
};

const day = new Date().toLocaleDateString('en-US', { timeZone: 'America/Phoenix', weekday: 'short', month: 'short', day: 'numeric' });
return [{ json: {
  subject: `Desert Comfort call check-up - ${day} - ${rows.length ? `${plural(rows.length, 'call')} flagged of ${calls}` : `all quiet (${plural(calls, 'call')})`}`,
  slack_text: `*Desert Comfort call check-up - daily digest (${day})*\n\n${build('slack')}`,
  email_text: build('email'),
  calls, flagged: rows.length, red: red.length, yellow: yellow.length, errors: err.length, walked_away: walkedTotal,
} }];
