"""Renders the two n8n Code nodes that write the "soonest time" sentence for check_availability_desertcomfort (non-emergency path).
Both come from one template (format_soonest.js); only the slots node they read and the night opening words differ.
    python availability_night/build.py [OUTDIR]     -> writes one .js file per node (the exact text to paste into the n8n node)"""
import sys
from pathlib import Path

HERE = Path(__file__).parent
NODES = {
    "Format Soonest Availability Standard": {"slots": "Get Available Slots GHL Standard", "lead": "The soonest I can get a technician out is"},
    "Format Soonest Availability Previous Technician": {"slots": "Get Available Slots GHL Previous Technician", "lead": "The soonest available is"},
}


def render(node_name: str) -> str:
    spec = NODES[node_name]
    code = (HERE / "format_soonest.js").read_bytes().decode("utf-8")
    assert code.count("__SLOTS_NODE__") == 1 and code.count("__NIGHT_LEAD__") == 1
    return code.replace("__SLOTS_NODE__", spec["slots"]).replace("__NIGHT_LEAD__", spec["lead"])


if __name__ == "__main__":
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else HERE / "rendered"
    out.mkdir(parents=True, exist_ok=True)
    for name in NODES:
        (out / (name.replace(" ", "_") + ".js")).write_bytes(render(name).encode("utf-8"))
        print("wrote", name)
