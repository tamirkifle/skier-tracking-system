#!/usr/bin/env python3
"""Summarise one fleet arm from the per-replica samples scripts/fleet.sh recorded.

Every figure is a delta of a counter over a window. Averaging the ``skier_admission_rate`` gauge
would report what each controller decided, and the claim under test is what the fleet let through.

The window is not a chosen constant. It opens at the first instant from which every replica reports
its admission rate at its configured floor and stays there, and ``settle-seconds`` is only a lower
bound on that search. Letting the system's own state define the window keeps two arms comparable
without a number that has to be re-justified whenever the scenario changes.

Usage: fleet-analyse.py <samples.csv> <replicas> <min-settle-seconds>
"""

import csv
import json
import os
import sys


def load(path):
    rows = []
    with open(path) as handle:
        for row in csv.DictReader(handle):
            try:
                rows.append(
                    {
                        "t": int(row["elapsed_ms"]) / 1000.0,
                        "replica": int(row["replica"]),
                        "accepted": float(row["accepted"]),
                        "shed": float(row["shed"]),
                        "rate": float(row["rate"]),
                        "floor": float(row["floor"]),
                        "pinned": float(row["floor_pinned"]),
                        "depth": float(row["depth"]),
                        "fleet_size": float(row["fleet_size"]),
                        "staleness": float(row["registry_staleness"]),
                        "refills": float(row.get("refill_invocations") or -1),
                        "refill_gap_max": float(row.get("refill_gap_max") or -1),
                        "discarded": float(row.get("permits_discarded") or -1),
                        "tokens": float(row.get("admission_tokens") or -1),
                    }
                )
            except (ValueError, KeyError):
                # A scrape that raced a container restart yields a short line. Dropping it is
                # correct; coercing it to zero would read as a replica that admitted nothing.
                continue
    return rows


def load_cpu(path, start, end):
    """Per-container CPU over the measured window, from the sibling ``docker-stats.csv``.

    "The host had no CPU left to give that replica" is one of the two answers to a replica
    admitting less than its own floor, so it has to be a number in the artifact. Optional: an arm
    recorded before the stats sampler existed has no such file.

    The stats sampler starts its own monotonic clock a moment after the scrape sampler's, so this
    is reported as a range over the window rather than aligned sample-for-sample with anything
    above.
    """
    stats = os.path.join(os.path.dirname(path) or ".", "docker-stats.csv")
    if not os.path.exists(stats):
        return None
    per_container = {}
    per_instant = {}
    with open(stats) as handle:
        for row in csv.DictReader(handle):
            try:
                at = int(row["elapsed_ms"]) / 1000.0
                cpu = float(row["cpu_percent"].rstrip("%"))
            except (ValueError, KeyError):
                continue
            if not start <= at <= end:
                continue
            per_container.setdefault(row["name"], []).append(cpu)
            per_instant[at] = per_instant.get(at, 0.0) + cpu
    if not per_instant:
        return None
    return {
        "instants": len(per_instant),
        "summed_mean_percent": round(sum(per_instant.values()) / len(per_instant), 1),
        "summed_max_percent": round(max(per_instant.values()), 1),
        "per_container_mean_percent": {
            name: round(sum(v) / len(v), 1)
            for name, v in sorted(per_container.items(), key=lambda kv: -sum(kv[1]) / len(kv[1]))
        },
    }


def window_start(rows, replicas, minimum):
    """First sample time at or after ``minimum`` from which every replica stays at its floor.

    "Stays" is the operative word: a single tick at the floor is also what a healthy controller
    passes through on its way back up, so a window opened on the first one could close around a
    recovery rather than around a saturation.
    """
    times = sorted({r["t"] for r in rows if r["t"] >= minimum})
    for candidate in times:
        tail = [r for r in rows if r["t"] >= candidate]
        at_floor = {r["replica"] for r in tail if r["rate"] > r["floor"]}
        if not at_floor and len({r["replica"] for r in tail}) == replicas:
            return candidate
    sys.exit(
        f"no window at or after {minimum:.0f}s in which all {replicas} replica(s) hold their floor;"
        " this arm never saturated, so its admission figures are not comparable to one that did"
    )


def main():
    path, replicas, settle = sys.argv[1], int(sys.argv[2]), float(sys.argv[3])
    rows = load(path)
    if not rows:
        sys.exit(f"no usable samples in {path}")

    end = max(r["t"] for r in rows)
    start = window_start(rows, replicas, settle)
    window = [r for r in rows if r["t"] >= start]
    if len(window) < 2 * replicas:
        sys.exit(f"window [{start:.0f}s, {end:.0f}s] holds too few samples ({len(window)})")

    span = max(r["t"] for r in window) - min(r["t"] for r in window)

    per_replica = []
    for i in range(1, replicas + 1):
        series = sorted((r for r in window if r["replica"] == i), key=lambda r: r["t"])
        if len(series) < 2:
            sys.exit(f"replica {i} produced {len(series)} sample(s) in the window")
        first, last = series[0], series[-1]
        dt = last["t"] - first["t"]
        per_replica.append(
            {
                "replica": i,
                "admitted": last["accepted"] - first["accepted"],
                "shed": last["shed"] - first["shed"],
                "admitted_per_second": (last["accepted"] - first["accepted"]) / dt if dt else 0.0,
                "rate_min": min(r["rate"] for r in series),
                "rate_max": max(r["rate"] for r in series),
                "floor": last["floor"],
                "floor_pinned_ticks": last["pinned"] - first["pinned"],
                # Min rather than last: a replica that ever believed it was alone used the whole
                # floor and the whole additive step for those ticks, and reporting only the final
                # value would hide it. Coordination working means this is N for the whole window.
                "fleet_size_min": min(r["fleet_size"] for r in series),
                "fleet_size_max": max(r["fleet_size"] for r in series),
                # The heartbeat's worst observed lateness, which is the diagnosis for a fleet_size
                # that drops mid-run: past the member TTL a live replica is pruned by its peers.
                "registry_staleness_max": max(r["staleness"] for r in series),
                # The three columns that make "admitted less than its own floor" attributable. A
                # refill loop achieving its nominal rate (1000/refill-interval-ms per second, so
                # 100/s at the shipped 10 ms tick) minted every permit the rate entitled it to,
                # and a shortfall then has to be demand or the discarded column. A loop achieving
                # less, or one gap of seconds, is the other answer: scheduleAtFixedRate repays
                # arrears in a burst and a bucket one second deep cannot hold them.
                "refills_per_second": (last["refills"] - first["refills"]) / dt if dt else 0.0,
                "refill_gap_max_seconds": max(r["refill_gap_max"] for r in series),
                "permits_discarded": last["discarded"] - first["discarded"],
                "permits_discarded_per_second": (
                    (last["discarded"] - first["discarded"]) / dt if dt else 0.0
                ),
                # tokens is a gauge, not a counter: it enters the identity as the difference
                # between the window's two endpoints, the same treatment as queue depth below,
                # rather than as a delta of a monotonic series.
                "tokens_delta_per_second": (
                    (last["tokens"] - first["tokens"]) / dt if dt else 0.0
                ),
                "samples": len(series),
            }
        )

    # The queue is shared, so any replica's view of it is the same queue. Replica 1's series is used
    # rather than an average across replicas: averaging two scrapes taken 100 ms apart would smooth
    # the very quantity, depth growth, that the arm is being compared on.
    depth_series = sorted((r for r in window if r["replica"] == 1), key=lambda r: r["t"])
    depth_dt = depth_series[-1]["t"] - depth_series[0]["t"]
    depth_growth = (depth_series[-1]["depth"] - depth_series[0]["depth"]) / depth_dt

    aggregate_admitted = sum(r["admitted_per_second"] for r in per_replica)

    result = {
        "replicas": replicas,
        "window_seconds": round(span, 1),
        "window_start_seconds": round(start, 1),
        "min_settle_seconds": settle,
        "per_replica": [
            {k: (round(v, 1) if isinstance(v, float) else v) for k, v in r.items()}
            for r in per_replica
        ],
        "aggregate_admitted_per_second": round(aggregate_admitted, 1),
        # This replica's share of the configured floor: with coordination on it is floor/N, so the
        # aggregate below is the configured value rather than N times it.
        "effective_floor_per_instance": per_replica[0]["floor"],
        "aggregate_floor": round(sum(r["floor"] for r in per_replica), 1),
        "observed_fleet_size": sorted({r["fleet_size_min"] for r in per_replica}
                                      | {r["fleet_size_max"] for r in per_replica}),
        "queue_depth_start": depth_series[0]["depth"],
        "queue_depth_end": depth_series[-1]["depth"],
        "queue_growth_per_second": round(depth_growth, 1),
        # Admitted minus the rate the backlog grew is what the consumer removed. Derived rather
        # than scraped from the consumer, so it is measured in the same units, over the same
        # window, from the same two instants as the admission figures it is compared against.
        "implied_drain_per_second": round(aggregate_admitted - depth_growth, 1),
        "container_cpu": load_cpu(path, start, end),
    }

    print(f"replicas                    {replicas}")
    print(
        f"window                      {start:.0f}s .. {end:.0f}s  ({span:.0f}s)"
        f"   [all replicas at their floor from {start:.0f}s]"
    )
    print("")
    for r in per_replica:
        print(
            f"  replica {r['replica']}   admitted {r['admitted']:>8,.0f}"
            f"  ({r['admitted_per_second']:>6.1f}/s)"
            f"   shed {r['shed']:>9,.0f}"
            f"   rate {r['rate_min']:.0f}..{r['rate_max']:.0f}"
            f"   floor-pinned ticks {r['floor_pinned_ticks']:>7,.0f}"
            f"   floor {r['floor']:.0f}"
            f"   fleet-size {r['fleet_size_min']:.0f}..{r['fleet_size_max']:.0f}"
            f"   heartbeat late by up to {r['registry_staleness_max']:.1f}s"
        )
        print(
            f"              refill loop {r['refills_per_second']:>6.1f} inv/s"
            f"   worst gap {r['refill_gap_max_seconds']:.2f}s"
            f"   permits discarded {r['permits_discarded']:>8,.0f}"
            f"  ({r['permits_discarded_per_second']:>6.1f}/s)"
            f"   tokens Δ {r['tokens_delta_per_second']:>6.1f}/s"
        )
        budget = (
            r["admitted_per_second"] + r["permits_discarded_per_second"] + r["tokens_delta_per_second"]
        )
        print(
            f"              admitted+discarded+tokens {budget:>6.1f}/s vs floor {r['floor']:.0f}"
        )
    print("")
    print(f"observed fleet size             {result['observed_fleet_size']}")
    print(f"effective floor per instance    {result['effective_floor_per_instance']:.0f} permits/s")
    print(f"aggregate floor (sum of shares) {result['aggregate_floor']:.0f} permits/s")
    print(f"aggregate admitted              {result['aggregate_admitted_per_second']:.1f} events/s")
    print(f"queue depth                     {result['queue_depth_start']:,.0f}"
          f" -> {result['queue_depth_end']:,.0f}"
          f"  ({result['queue_growth_per_second']:+.1f}/s)")
    print(f"implied consumer drain          {result['implied_drain_per_second']:.1f} events/s")
    cpu = result["container_cpu"]
    if cpu:
        print("")
        print(
            f"container CPU over the window   summed mean {cpu['summed_mean_percent']:.0f}%"
            f"  peak {cpu['summed_max_percent']:.0f}%"
            f"   ({cpu['instants']} instants; 100% = one core."
            " The load generator runs on the host and is not in this total)"
        )
        for name, mean in cpu["per_container_mean_percent"].items():
            print(f"    {name:<24} {mean:>7.1f}%")

    out = os.path.join(os.path.dirname(path) or ".", "result.json")
    with open(out, "w") as handle:
        json.dump(result, handle, indent=2)
    print("")
    print(f"result: {out}")


if __name__ == "__main__":
    main()
