#!/usr/bin/env python3
"""Join src/*.py (in name order) into skills/data-context/scripts/datawright.py, the one file users get.
`python tools/build.py` writes it; `--check` exits 1 if the file is stale (CI runs this)."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "skills/data-context/scripts/datawright.py"
text = "".join(p.read_text() for p in sorted((ROOT / "src").glob("*.py")))
if "--check" in sys.argv:
    sys.exit(0 if OUT.read_text() == text else f"{OUT.relative_to(ROOT)} is stale: run `python tools/build.py`")
OUT.write_text(text)
print(f"wrote {OUT.relative_to(ROOT)} from {len(list((ROOT / 'src').glob('*.py')))} parts")
