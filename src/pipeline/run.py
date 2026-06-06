"""CLI: orquesta bronze → silver → gold → models."""
from __future__ import annotations

import argparse
import time

from . import bronze, gold, models, silver


STEPS = {
    "bronze": bronze.run,
    "silver": silver.run,
    "gold": gold.run,
    "models": models.run,
}


def main():
    parser = argparse.ArgumentParser(description="Supermarket pipeline orchestrator")
    parser.add_argument("--step", choices=list(STEPS) + ["all"], default="all")
    args = parser.parse_args()

    if args.step != "all":
        step_order = [args.step]
    else:
        step_order = list(STEPS)
    i = 0
    while i < len(step_order):
        step_name = step_order[i]
        start_time = time.perf_counter()
        print(f"\n=== running step: {step_name} ===")
        STEPS[step_name]()
        print(f"=== {step_name} done in {time.perf_counter() - start_time:.1f}s ===")
        i += 1


if __name__ == "__main__":
    main()
