# ---------------------------------------------------------------- updates

def version_of(text):
    m = re.search(r'^VERSION = "([\d.]+)"', text, re.M)
    return tuple(map(int, m[1].split("."))) if m else (0,)


def installed_copies():
    """[(version, path)] of datawright.py in installed copies of the data-context skill, newest first."""
    home = Path.home()
    bases = [Path(os.environ.get("CLAUDE_CONFIG_DIR", home / ".claude")), home / ".agents", home / ".codex", home / ".grok",
             ROOT / ".claude", ROOT / ".agents", ROOT / ".codex", ROOT / ".grok"]
    found = {p.resolve() for b in bases if b.is_dir() for p in b.glob("**/data-context/scripts/datawright.py")}
    return sorted(((version_of(p.read_text(errors="replace")), p) for p in found), reverse=True)


def update(source=None):
    """Replace this repo's .data-context/datawright.py with the newest installed copy of the skill (or with source)."""
    target = HOME / "datawright.py"
    current = version_of(target.read_text(errors="replace")) if target.exists() else (0,)
    cands = [(version_of(Path(source).read_text(errors="replace")), Path(source))] if source else installed_copies()
    if not cands:
        sys.exit("No installed copy of the data-context skill found. Update the datawright skills "
                 "(Claude Code: /plugin update; others: npx skills update), or pass --from <path to datawright.py>.")
    new, path = cands[0]
    if new <= current:
        return print(f"datawright {'.'.join(map(str, current))} is up to date (newest installed: {'.'.join(map(str, new))}, {path})")
    import shutil
    shutil.copy(path, target)
    print(f"updated .data-context/datawright.py {'.'.join(map(str, current))} -> {'.'.join(map(str, new))} from {path}; now run: datawright build")


