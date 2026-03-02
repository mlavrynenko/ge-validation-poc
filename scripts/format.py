import subprocess
import sys

commands = [
    ["ruff", "check", ".", "--fix"],
]

for cmd in commands:
    print("▶", " ".join(cmd))
    if subprocess.run(cmd).returncode != 0:
        sys.exit(1)

print("✅ Python formatting complete")
print("ℹ️ YAML is validated via yamllint (no auto-fix)")
