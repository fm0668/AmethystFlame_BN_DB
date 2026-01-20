from __future__ import annotations

from pathlib import Path


def main() -> None:
    root = Path(__file__).resolve().parent
    template = root / ".env.example"
    target = root / ".env"

    if target.exists():
        return
    if not template.exists():
        raise SystemExit("missing .env.example")
    target.write_text(template.read_text(encoding="utf-8"), encoding="utf-8")


if __name__ == "__main__":
    main()

