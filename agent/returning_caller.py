"""returning_caller.py: how the agent treats a caller we already hold a contact for but have not booked (pure Python, no network, no secrets).

Why it exists (real call, 2026-10-05): the n8n lookup FOUND the contact (stage "qualified": called before, quoted, did not book) and then
threw it away, because only booked or served contacts counted as "existing". The agent greeted a known person as a stranger. Now the lookup
can answer route = "returning_caller" with the name and address on file, and this module turns that into:
  - a greeting by first name ("Hi Callum, ... good to have you calling back") when a usable first name is on file, otherwise the normal greeting
  - a short "Returning Caller" block for the Voice Layer: what is on file (exact values), how to CONFIRM it instead of asking again, and a hard
    rule never to invent anything that is not on file (a missing field is shown as "(not on file)")
The wording of the greeting and of the block lives in the prompt file (so it can be edited like the rest of the prompt); this module only
extracts it, decides which case applies and fills the values in. A returning caller still runs on the NEW-CALLER instructions.

Used by desert_comfort_agent.py through a guarded import; if this file is missing every caller is treated exactly as before. Never raises.
"""
from __future__ import annotations

import re

ROUTE = "returning_caller"
NOT_ON_FILE = "(not on file)"
BLOCK_START = "<!-- RETURNING_BLOCK_START -->"
BLOCK_END = "<!-- RETURNING_BLOCK_END -->"
OPENING_MARKER = "### Returning-caller opening line"

# values that are placeholders, not a real address
_PLACEHOLDER_ADDRESS_RE = re.compile(r"^(n/?a|none|null|nil|undefined|unknown|not captured|not on file|no address|tbd|-+)$", re.I)


def usable_address(value) -> str:
    """The address if it looks like a real one (at least a short street line with a letter and a digit or number word), else ''."""
    try:
        s = re.sub(r"\s+", " ", "" if value is None else str(value)).strip()
        if len(s) < 6 or len(s) > 200 or _PLACEHOLDER_ADDRESS_RE.match(s) or not re.search(r"[A-Za-z]", s):
            return ""
        return s
    except Exception:
        return ""


def plan(lookup, clean_name, normalize_address=lambda s: s) -> dict:
    """What to do with this lookup result.

    clean_name: the agent's name filter (drops placeholders like "Not captured", phone numbers, junk). normalize_address: spoken numbers to digits.
    Returns {"returning": bool, "first_name", "last_name", "address", "reason"}; "returning" is True only when the route says so AND at
    least one of first name / address is usable."""
    out = {"returning": False, "first_name": "", "last_name": "", "address": "", "reason": ""}
    try:
        if not isinstance(lookup, dict) or lookup.get("route") != ROUTE:
            out["reason"] = "route is not returning_caller"
            return out
        first = clean_name(lookup.get("first_name")) or ""
        last = clean_name(lookup.get("last_name")) or ""
        address = usable_address(lookup.get("service_address"))
        address = normalize_address(address) if address else ""
        if not first and not address:
            out["reason"] = "returning_caller but no usable name or address on file"
            return out
        out.update(returning=True, first_name=first, last_name=last, address=address, reason="ok")
        return out
    except Exception:
        out["reason"] = "error while planning"
        return out


def confirm_instruction(p: dict) -> str:
    """One sentence telling the model how to confirm what is on file, with the REAL values in the example (never a made-up sample)."""
    first, address = p.get("first_name", ""), p.get("address", "")
    if first and address:
        return (f'When you reach the contact details, confirm both in ONE sentence, for example: "I have {first} at {address} on file - is that still '
                f'right?" Do not ask for the name and the address separately.')
    if first:
        return "The opening line already used the first name. When you reach the contact details, ask only for the service address."
    return (f'When you reach the contact details, confirm the address in ONE sentence, for example: "I have {address} on file - is that still right?" '
            f"Then ask for the caller's name.")


def extract_block_template(prompt_text: str) -> str:
    """The Returning Caller block from the prompt file (between the two HTML comment markers). '' if it is not there."""
    try:
        start = prompt_text.index(BLOCK_START) + len(BLOCK_START)
        end = prompt_text.index(BLOCK_END, start)
        return prompt_text[start:end].strip()
    except Exception:
        return ""


def extract_returning_opening(prompt_text: str) -> str:
    """The returning-caller opening line (the first quoted blockquote after the marker). '' if it is not there."""
    try:
        after = prompt_text[prompt_text.index(OPENING_MARKER) + len(OPENING_MARKER):]
        for line in after.splitlines():
            line = line.strip()
            if line.startswith(">"):
                return line[line.index('"') + 1: line.rindex('"')]
            if line.startswith("## "):
                break
    except Exception:
        pass
    return ""


def render_block(template: str, p: dict) -> str:
    """The block for the Voice Layer. '' for anyone who is not a returning caller, or if the template is missing."""
    try:
        if not p.get("returning") or not template:
            return ""
        values = {
            "on_file_first_name": p.get("first_name") or NOT_ON_FILE,
            "on_file_last_name": p.get("last_name") or NOT_ON_FILE,
            "on_file_address": p.get("address") or NOT_ON_FILE,
            "on_file_confirm_instruction": confirm_instruction(p),
        }
        out = template
        for key, value in values.items():
            out = out.replace("{{" + key + "}}", value)
        return out
    except Exception:
        return ""


def choose_opening(p: dict, normal_line: str, returning_template: str) -> str:
    """The greeting to speak. The returning greeting only when a usable first name is on file and the template exists; otherwise the normal one."""
    try:
        if p.get("returning") and p.get("first_name") and returning_template:
            return returning_template.replace("{{first_name}}", p["first_name"])
    except Exception:
        pass
    return normal_line
