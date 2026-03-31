"""
scripts/download_logos.py

One-shot script to download all NBA team logos into app/static/logos/.

Usage:
    python scripts/download_logos.py          # skip existing files
    python scripts/download_logos.py --force  # re-download everything
"""

import sys
import os
import argparse

# Allow running from the project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services import logos as logos_svc  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description="Download NBA team logos")
    parser.add_argument("--force", action="store_true", help="Re-download even if files exist")
    args = parser.parse_args()

    results = logos_svc.download_all(force=args.force)

    ok   = sum(1 for v in results.values() if v)
    fail = sum(1 for v in results.values() if not v)

    print(f"Done: {ok} downloaded, {fail} failed")
    if fail:
        failed = [abbr for abbr, ok in results.items() if not ok]
        print("Failed:", ", ".join(failed))


if __name__ == "__main__":
    main()
