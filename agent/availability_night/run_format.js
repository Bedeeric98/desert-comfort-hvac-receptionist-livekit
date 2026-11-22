// Test harness for format_soonest.js: runs the n8n Code-node body against fake node data and a frozen clock.
// stdin: {"code": "...", "now": "2026-10-15T11:44:00Z", "nodes": {"<node name>": <that node's json>}}   stdout: the node's return value as JSON.
// A node name that is absent from "nodes" makes $("name") throw, exactly like a node that did not run in n8n.
const fs = require("fs");
const input = JSON.parse(fs.readFileSync(0, "utf8"));
const RealDate = Date;
const fixed = new RealDate(input.now).getTime();
class FakeDate extends RealDate {
  constructor(...a) { if (a.length === 0) super(fixed); else super(...a); }
  static now() { return fixed; }
}
globalThis.Date = FakeDate;
const nodes = input.nodes;
const $ = (name) => {
  if (!(name in nodes)) throw new Error("no node named " + name);
  return { first: () => ({ json: nodes[name] }) };
};
const out = new Function("$", input.code)($);
console.log(JSON.stringify(out));
