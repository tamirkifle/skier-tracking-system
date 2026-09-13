#!/usr/bin/env python3
"""Summarise a Redis-outage arm from the per-replica samples scripts/fleet-redis-outage.sh recorded.

The claim under test is a negative one: taking Redis away does not change what each replica
believes the fleet size to be. A fall back to 1 shows up as ``fleet_size`` dropping and ``floor``
rising to the whole configured value, so both are printed per phase rather than averaged.

Staleness is a max per phase and monotonic-or-not, not a mean: a held value has to be tellable
apart from a fresh one at any single scrape. The replica set is read from the data rather than from
the argument, because the two ``coldstart`` phases add one.

Usage: fleet-redis-outage-analyse.py <samples.csv> <replicas>
"""

import collections
import csv
import json
import os
import sys

PHASES = (
    "baseline",
    "redis-down",
    "redis-restored",
    "coldstart-down",
    "coldstart-restored",
)


def load(path):
    rows = []
    with open(path) as handle:
        for row in csv.DictReader(handle):
            try:
                rows.append(
                    {
                        "t": int(row["elapsed_ms"]) / 1000.0,
                        "phase": row["phase"],
                        "replica": int(row["replica"]),
                        "fleet_size": float(row["fleet_size"]),
                        "floor": float(row["floor"]),
                        "staleness": float(row["registry_staleness"]),
                        "failures": float(row["registry_failures"]),
                        "shrinks": float(row["registry_shrinks"]),
                        "rate": float(row["rate"]),
                        "readiness": row["readiness"].strip(),
                    }
                )
            except (ValueError, KeyError):
                # A scrape that timed out or raced a container yields a short line. Counted below
                # rather than dropped silently, because an unreachable replica is itself a result:
                # the run has to show whether stopping Redis took a server container down.
                rows.append({"phase": row.get("phase", "?"), "unreadable": True,
                             "readiness": row.get("readiness", "").strip()})
    return rows


def monotonic_nondecreasing(values):
    return all(b >= a for a, b in zip(values, values[1:]))


def configured_floor(good):
    """The fleet-wide floor, reconstructed as ``floor x fleet_size``.

    Not read from the configuration and not taken as a max over the observed floors. ``floor`` is
    this instance's share, so the product is invariant: a replica that believes it is alone
    reports 100 x 1 and one that has counted its peer reports 50 x 2. A max instead lets a startup
    transient inflate the configured value, which makes "did the floor stay put" true for any
    observation at all, and a check that cannot fail has not passed.
    """
    products = collections.Counter(round(r["floor"] * r["fleet_size"]) for r in good)
    return float(products.most_common(1)[0][0]) if products else 0.0


def summarise(rows, replicas):
    good = [r for r in rows if not r.get("unreadable")]
    configured = configured_floor(good)
    # FleetRegistry.share: integer division, floored at 1.
    expected_share = max(1.0, float(int(configured) // replicas))
    result = {
        "samples": len(rows),
        "unreadable_samples": len(rows) - len(good),
        "replicas": replicas,
        "configured_floor": configured,
        "expected_share": expected_share,
        "phases": {},
    }
    for phase in PHASES:
        per_replica = []
        present = sorted({r["replica"] for r in good if r["phase"] == phase})
        for i in present:
            series = [r for r in good if r["phase"] == phase and r["replica"] == i]
            if not series:
                continue
            stale = [r["staleness"] for r in series]
            per_replica.append(
                {
                    "replica": i,
                    "samples": len(series),
                    "fleet_size_min": min(r["fleet_size"] for r in series),
                    "fleet_size_max": max(r["fleet_size"] for r in series),
                    "floor_min": min(r["floor"] for r in series),
                    "floor_max": max(r["floor"] for r in series),
                    "floor_last": series[-1]["floor"],
                    "staleness_first": stale[0],
                    "staleness_max": max(stale),
                    "staleness_last": stale[-1],
                    # Monotonic within a phase is the shape that matters: with Redis down every
                    # refresh fails, so the gauge should only rise. A dip means a refresh
                    # succeeded, which would mean Redis was not actually away.
                    "staleness_monotonic": monotonic_nondecreasing(stale),
                    "failures_delta": (
                        max(r["failures"] for r in series)
                        - min(r["failures"] for r in series)
                    ),
                    "shrinks_delta": (
                        max(r["shrinks"] for r in series) - min(r["shrinks"] for r in series)
                    ),
                    "readiness_codes": sorted({r["readiness"] for r in series}),
                    "rate_min": min(r["rate"] for r in series),
                    "rate_max": max(r["rate"] for r in series),
                }
            )
        unread = len([r for r in rows if r.get("unreadable") and r.get("phase") == phase])
        result["phases"][phase] = {"per_replica": per_replica, "unreadable_samples": unread}

    down = result["phases"].get("redis-down", {}).get("per_replica", [])
    # The acceptance question, answered as a boolean over the whole down phase rather than left
    # for a reader to eyeball out of the table: did every replica keep believing there are N of
    # them, and did every replica's floor stay at the share rather than at the whole configured
    # value? Both bounds, not just the minimum: a size that moved in either direction is a refresh
    # that succeeded, which would mean Redis was not actually away.
    result["held_through_outage"] = bool(down) and all(
        r["fleet_size_min"] == replicas
        and r["fleet_size_max"] == replicas
        and r["floor_min"] == expected_share
        and r["floor_max"] == expected_share
        for r in down
    )
    result["failed_open_to_one"] = any(r["fleet_size_min"] <= 1 for r in down)
    result["staleness_rose"] = bool(down) and all(
        r["staleness_max"] > r["staleness_first"] and r["staleness_monotonic"] for r in down
    )
    result["failures_counted"] = bool(down) and all(r["failures_delta"] > 0 for r in down)

    # The coldstart replica is reported on its own rather than folded into the phase table,
    # because the claim about it is the opposite one: it is expected to read a fleet size of 1 and
    # take the whole configured floor.
    cold = result["phases"].get("coldstart-down", {}).get("per_replica", [])
    newcomer = [r for r in cold if r["replica"] > replicas]
    # Summing every replica's floor regardless of readiness would report the unfixed number for a
    # fix that works: an instance that has never observed the fleet size withholds readiness, so
    # an orchestrator routes nothing to it and its floor never reaches the broker.
    serving = [r for r in cold if "200" in r["readiness_codes"]]
    result["coldstart"] = {
        "replica": newcomer[0]["replica"] if newcomer else None,
        "fleet_size": [newcomer[0]["fleet_size_min"], newcomer[0]["fleet_size_max"]]
        if newcomer
        else None,
        "floor": [newcomer[0]["floor_min"], newcomer[0]["floor_max"]] if newcomer else None,
        "staleness_first": newcomer[0]["staleness_first"] if newcomer else None,
        "readiness_codes": newcomer[0]["readiness_codes"] if newcomer else [],
        "aggregate_floor_while_cold": sum(r["floor_max"] for r in cold) if cold else None,
        "aggregate_floor_while_cold_serving": (
            sum(r["floor_max"] for r in serving) if cold else None
        ),
        "serving_replicas_while_cold": [r["replica"] for r in serving],
        "newcomer_served_while_cold": bool(newcomer)
        and "200" in newcomer[0]["readiness_codes"],
    }
    restored = result["phases"].get("coldstart-restored", {}).get("per_replica", [])
    result["coldstart"]["aggregate_floor_after_recovery"] = (
        sum(r["floor_min"] for r in restored) if restored else None
    )
    # The min over the phase is not the recovered value once the newcomer starts the phase at a
    # floor of 1. An unsized instance takes the share of an arbitrarily large fleet, so the min is
    # that 1, and the sum then reads below the configured floor for a run that recovered
    # correctly. The last sample is what "after recovery" was meant to say.
    result["coldstart"]["aggregate_floor_at_end_of_recovery"] = (
        sum(r["floor_last"] for r in restored) if restored else None
    )
    result["shrinks_observed"] = sum(
        r["shrinks_delta"]
        for phase in result["phases"].values()
        for r in phase["per_replica"]
    )
    return result


def render(result):
    print()
    print(f"replicas                        {result['replicas']}")
    print(f"samples                         {result['samples']}"
          f"  ({result['unreadable_samples']} unreadable)")
    print(f"configured floor                {result['configured_floor']:.0f}/s"
          f"  (floor x fleet_size), share {result['expected_share']:.0f}/s")
    for phase in PHASES:
        block = result["phases"][phase]
        print()
        print(f"--- {phase} ---")
        if not block["per_replica"]:
            print("   (no samples)")
            continue
        for r in block["per_replica"]:
            print(
                f"   replica {r['replica']}: {r['samples']:3d} samples"
                f"   fleet-size {r['fleet_size_min']:.0f}..{r['fleet_size_max']:.0f}"
                f"   floor {r['floor_min']:.0f}..{r['floor_max']:.0f}/s"
                f"   staleness {r['staleness_first']:.1f}->{r['staleness_last']:.1f}s"
                f" (max {r['staleness_max']:.1f}, monotonic {r['staleness_monotonic']})"
                f"   refresh-failures +{r['failures_delta']:.0f}"
                f"   shrinks +{r['shrinks_delta']:.0f}"
                f"   readiness {','.join(r['readiness_codes'])}"
            )
        if block["unreadable_samples"]:
            print(f"   unreadable scrapes: {block['unreadable_samples']}")
    print()
    print(f"held the fleet size through the outage    {result['held_through_outage']}")
    print(f"fell back to a fleet size of 1            {result['failed_open_to_one']}")
    print(f"staleness rose monotonically while down   {result['staleness_rose']}")
    print(f"refresh failures were counted             {result['failures_counted']}")
    print(f"successful refreshes that under-counted   {result['shrinks_observed']:.0f}"
          f"  (skier_fleet_registry_shrink_total)")
    cold = result.get("coldstart") or {}
    if cold.get("replica"):
        print()
        print("--- the replica started into the outage ---")
        print(f"   replica {cold['replica']}"
              f"   fleet-size {cold['fleet_size'][0]:.0f}..{cold['fleet_size'][1]:.0f}"
              f"   floor {cold['floor'][0]:.0f}..{cold['floor'][1]:.0f}/s"
              f"   first staleness reading {cold['staleness_first']:.1f}s"
              f"   readiness {','.join(cold['readiness_codes'])}")
        print(f"   aggregate floor while it is cold         "
              f"{cold['aggregate_floor_while_cold']:.0f}/s"
              f"   against a configured {result['configured_floor']:.0f}/s")
        print(f"   ... of the replicas that are serving     "
              f"{cold['aggregate_floor_while_cold_serving']:.0f}/s"
              f"   (readiness 200 at any point: "
              f"{','.join(str(i) for i in cold['serving_replicas_while_cold']) or 'none'})")
        print(f"   the newcomer served traffic while cold   "
              f"{cold['newcomer_served_while_cold']}")
        if cold.get("aggregate_floor_after_recovery") is not None:
            print(f"   aggregate floor once Redis is back       "
                  f"{cold['aggregate_floor_after_recovery']:.0f}/s  (min over the phase)")
            print(f"   ... at the end of the recovery phase     "
                  f"{cold['aggregate_floor_at_end_of_recovery']:.0f}/s")
    print()


def main():
    if len(sys.argv) != 3:
        print(__doc__.strip().splitlines()[-1], file=sys.stderr)
        return 2
    samples, replicas = sys.argv[1], int(sys.argv[2])
    result = summarise(load(samples), replicas)
    render(result)
    with open(os.path.join(os.path.dirname(samples) or ".", "result.json"), "w") as handle:
        json.dump(result, handle, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
