from __future__ import annotations

import csv
import json
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from accounts.pincode_directory import PINCODE_DIRECTORY_PATH
from accounts.pincode_directory import _canon_state_name


class Command(BaseCommand):
    help = (
        "Build accounts/data/india_pincode_directory.json (PIN -> State mapping) from a CSV file.\n\n"
        "The CSV must contain at least two columns: one for PIN code and one for State/UT.\n"
        "Common headers supported: pincode, pin_code, pin, postal_code and state, state_name, statename."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--input",
            required=True,
            help="Path to the source CSV containing all-India PIN directory data.",
        )
        parser.add_argument(
            "--output",
            default=str(PINCODE_DIRECTORY_PATH),
            help=f"Output JSON path (default: {PINCODE_DIRECTORY_PATH}).",
        )

    def handle(self, *args, **options):
        input_path = Path(options["input"]).expanduser().resolve()
        if not input_path.exists():
            raise CommandError(f"Input CSV not found: {input_path}")

        output_path = Path(options["output"]).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with input_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise CommandError("CSV appears to have no header row.")

            # Detect columns
            fn_lower = {name.lower(): name for name in reader.fieldnames}

            pin_col = None
            for cand in ["pincode", "pin_code", "pin", "postal_code", "postalcode", "pin code"]:
                if cand in fn_lower:
                    pin_col = fn_lower[cand]
                    break
            if not pin_col:
                for name in reader.fieldnames:
                    if "pin" in name.lower():
                        pin_col = name
                        break

            state_col = None
            for cand in ["state", "state_name", "statename", "state/ut", "state_ut", "circle", "circlename"]:
                if cand in fn_lower:
                    state_col = fn_lower[cand]
                    break
            if not state_col:
                for name in reader.fieldnames:
                    if "state" in name.lower():
                        state_col = name
                        break

            if not pin_col or not state_col:
                raise CommandError(
                    "Could not auto-detect PIN/State columns.\n"
                    f"Headers found: {reader.fieldnames}\n"
                    "Expected something like 'pincode' and 'state'."
                )

            mapping: dict[str, str] = {}

            for row in reader:
                pin_raw = (row.get(pin_col) or "").strip()
                pin = "".join(ch for ch in pin_raw if ch.isdigit())
                if len(pin) != 6:
                    continue

                state = _canon_state_name(row.get(state_col) or "")
                if not state:
                    continue

                mapping[pin] = state

        with output_path.open("w", encoding="utf-8") as out:
            json.dump(mapping, out, ensure_ascii=False, indent=2, sort_keys=True)

        self.stdout.write(self.style.SUCCESS(f"Wrote {len(mapping):,} PIN entries to {output_path}"))
