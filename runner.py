#!/usr/bin/env python3
"""runner.py — JSON-in/JSON-out wrapper for FinancePy bond pricing."""

# TO RUN:
# From inside the repo root:
# `python runner.py < finance_py_input.json`

import argparse
import json
import sys

try:
    from financepy.utils.date import Date
    from financepy.utils.frequency import FrequencyTypes
    from financepy.utils.day_count import DayCountTypes
    from financepy.products.bonds.bond import Bond
except ModuleNotFoundError as exc:
    missing = getattr(exc, "name", None) or str(exc)
    pyver = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"

    msg_lines = [
        "Failed to import FinancePy dependencies.",
        f"Missing module: {missing}",
        f"Python: {pyver}",
        "",
        "Fix:",
        "- Use Python 3.10–3.12 (FinancePy does not currently support 3.13+).",
        "- Create/activate a venv, then install FinancePy deps (editable install is fine):",
        "    python3.12 -m venv .venv",
        "    source .venv/bin/activate",
        "    python -m pip install -U pip",
        "    python -m pip install -e .",
        "",
        "Then run:",
        "    python runner.py < input.json",
    ]
    print("\n".join(msg_lines), file=sys.stderr)
    raise


# ---------------------------------------------------------------------------
# Default bond parameters used when FinancePy runs as a downstream step
# (e.g., after DicePy in a FaIR → DicePy → FinancePy chain). In that case
# only climate risk fields arrive in the input; bond specifics are absent.
# When running standalone, supply these keys in the input JSON to override.
# ---------------------------------------------------------------------------
DEFAULT_ISSUE_DATE = [2024, 1, 1]
DEFAULT_MATURITY_DATE = [2034, 1, 1]    # 10-year bond
DEFAULT_SETTLEMENT_DATE = [2024, 1, 3]  # T+2 settlement
DEFAULT_COUPON_RATE = 0.04              # 4 % annual coupon


def _mean_nested(d: dict) -> float:
    """Collapse a {scenario: {config: float}} dict to a single mean value.

    DicePy outputs climate_risk_premium and adjusted_ytm in this shape.
    We average across all scenario/config combinations to get one scalar
    yield suitable for a single bond pricing call.
    """
    values = [v for inner in d.values() for v in inner.values()]
    return sum(values) / len(values) if values else 0.0


FREQ_MAP = {
    "annual": FrequencyTypes.ANNUAL,
    "semi_annual": FrequencyTypes.SEMI_ANNUAL,
    "quarterly": FrequencyTypes.QUARTERLY,
    "monthly": FrequencyTypes.MONTHLY,
}

DC_MAP = {
    "act_act_isda": DayCountTypes.ACT_ACT_ISDA,
    "thirty_e_360": DayCountTypes.THIRTY_E_360,
    "act_360": DayCountTypes.ACT_360,
    "act_365f": DayCountTypes.ACT_365F,
}


def run(params: dict) -> dict:
    # Bond parameters — use input values if present, fall back to defaults.
    # Defaults exist so this runner can be chained after DicePy without
    # requiring a full bond spec in the input.
    issue = params.get("issue_date", DEFAULT_ISSUE_DATE)      # [year, month, day]
    mat = params.get("maturity_date", DEFAULT_MATURITY_DATE)
    settle = params.get("settlement_date", DEFAULT_SETTLEMENT_DATE)

    issue_dt = Date(issue[2], issue[1], issue[0])
    maturity_dt = Date(mat[2], mat[1], mat[0])
    settle_dt = Date(settle[2], settle[1], settle[0])

    coupon = params.get("coupon_rate", DEFAULT_COUPON_RATE)
    freq = FREQ_MAP[params.get("frequency", "semi_annual")]
    dc = DC_MAP[params.get("day_count", "act_act_isda")]

    bond = Bond(issue_dt, maturity_dt, coupon, freq, dc)

    # ytm resolution: prefer an explicit scalar in the input (standalone use).
    # When chaining from DicePy, fall back to adjusted_ytm (baseline YTM +
    # climate risk premium) if available, then climate_risk_premium alone.
    # Both are {scenario: {config: float}} dicts — _mean_nested collapses
    # them to a single representative scalar for a single bond pricing call.
    ytm = params.get("ytm")
    if ytm is None:
        for key in ("adjusted_ytm", "climate_risk_premium"):
            val = params.get(key)
            if isinstance(val, dict):
                ytm = _mean_nested(val)
                break

    clean_price = params.get("clean_price")

    result = {}

    if ytm is not None:
        # Price from yield
        result["clean_price"] = bond.clean_price_from_ytm(settle_dt, ytm)
        result["dirty_price"] = bond.dirty_price_from_ytm(settle_dt, ytm)
        result["accrued_interest"] = bond.accrued_interest(settle_dt)
        result["macauley_duration"] = bond.macauley_duration(settle_dt, ytm)
        result["modified_duration"] = bond.modified_duration(settle_dt, ytm)
        result["convexity"] = bond.convexity_from_ytm(settle_dt, ytm)
        result["ytm"] = ytm
    elif clean_price is not None:
        # Yield from price
        ytm_calc = bond.yield_to_maturity(settle_dt, clean_price)
        result["ytm"] = ytm_calc
        result["clean_price"] = clean_price
        result["dirty_price"] = bond.dirty_price_from_ytm(settle_dt, ytm_calc)
        result["accrued_interest"] = bond.accrued_interest(settle_dt)
        result["macauley_duration"] = bond.macauley_duration(settle_dt, ytm_calc)
        result["modified_duration"] = bond.modified_duration(settle_dt, ytm_calc)
        result["convexity"] = bond.convexity_from_ytm(settle_dt, ytm_calc)
    else:
        raise ValueError("Provide either 'ytm' or 'clean_price'")

    return result


def _load_input_json() -> dict:
    parser = argparse.ArgumentParser(
        description="FinancePy runner: read bond inputs as JSON and emit JSON results."
    )
    parser.add_argument(
        "input",
        nargs="?",
        default="-",
        help="Path to input JSON file, or '-' to read from stdin (default).",
    )
    args = parser.parse_args()

    if args.input == "-":
        return json.load(sys.stdin)

    with open(args.input, "r", encoding="utf-8") as handle:
        return json.load(handle)


if __name__ == "__main__":
    input_data = _load_input_json()
    output_data = run(input_data)
    json.dump(output_data, sys.stdout, indent=2)
    print()  # trailing newline