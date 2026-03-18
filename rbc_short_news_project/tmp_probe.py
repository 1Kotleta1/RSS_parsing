import re
from pathlib import Path

text = Path("module_42660.txt").read_text(encoding="utf-8", errors="ignore")

assignments = re.findall(r"([A-Za-z_$][A-Za-z0-9_$]{0,4})=\"([^\"]+)\"", text)
for var, value in assignments:
    low = value.lower()
    if any(k in low for k in ["http", "rbc", "api", "v1", "v2", "short_news", "track"]):
        print(var, "=", value)
