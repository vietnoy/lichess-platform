#!/usr/bin/env python3
"""Summarize Vertex AI Gemini usage from Cloud Monitoring.

Requires an authenticated gcloud account with Monitoring read access:
  gcloud auth login
  python ops/vertex_usage.py --project chess-platform-497604 --days 7
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any


INPUT_USD_PER_MILLION = Decimal("0.30")
OUTPUT_USD_PER_MILLION = Decimal("2.50")


def _access_token() -> str:
    result = subprocess.run(
        ["gcloud", "auth", "print-access-token"],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return result.stdout.strip()


def _fetch_timeseries(project: str, metric: str, days: int, token: str) -> list[dict[str, Any]]:
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    params = {
        "filter": (
            f'metric.type="aiplatform.googleapis.com/publisher/online_serving/{metric}" '
            'AND resource.type="aiplatform.googleapis.com/PublisherModel"'
        ),
        "interval.startTime": start.isoformat().replace("+00:00", "Z"),
        "interval.endTime": end.isoformat().replace("+00:00", "Z"),
        "aggregation.alignmentPeriod": f"{days * 86400}s",
        "aggregation.perSeriesAligner": "ALIGN_SUM",
    }
    url = (
        f"https://monitoring.googleapis.com/v3/projects/{project}/timeSeries?"
        + urllib.parse.urlencode(params)
    )
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return payload.get("timeSeries", [])


def _point_value(series: dict[str, Any]) -> int:
    points = series.get("points") or []
    if not points:
        return 0
    value = points[0].get("value", {})
    return int(value.get("int64Value") or value.get("doubleValue") or 0)


def _money(value: Decimal) -> str:
    return f"${value.quantize(Decimal('0.000001'))}"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default="chess-platform-497604")
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()

    token = _access_token()
    invocations = _fetch_timeseries(args.project, "model_invocation_count", args.days, token)
    tokens = _fetch_timeseries(args.project, "token_count", args.days, token)

    request_rows: dict[tuple[str, str], int] = defaultdict(int)
    for series in invocations:
        resource = series.get("resource", {}).get("labels", {})
        key = (resource.get("model_user_id", "unknown"), resource.get("location", "unknown"))
        request_rows[key] += _point_value(series)

    token_rows: dict[tuple[str, str, str], int] = defaultdict(int)
    for series in tokens:
        resource = series.get("resource", {}).get("labels", {})
        metric_labels = series.get("metric", {}).get("labels", {})
        key = (
            resource.get("model_user_id", "unknown"),
            resource.get("location", "unknown"),
            metric_labels.get("type", "unknown"),
        )
        token_rows[key] += _point_value(series)

    print(f"Vertex AI usage for project={args.project}, window={args.days}d")
    print()
    print("model,location,requests,input_tokens,output_tokens,estimated_usd")

    keys = set(request_rows)
    keys.update((model, location) for model, location, _ in token_rows)
    total_cost = Decimal("0")
    for model, location in sorted(keys):
        input_tokens = token_rows.get((model, location, "input"), 0)
        output_tokens = token_rows.get((model, location, "output"), 0)
        cost = (
            Decimal(input_tokens) / Decimal(1_000_000) * INPUT_USD_PER_MILLION
            + Decimal(output_tokens) / Decimal(1_000_000) * OUTPUT_USD_PER_MILLION
        )
        total_cost += cost
        print(
            f"{model},{location},{request_rows.get((model, location), 0)},"
            f"{input_tokens},{output_tokens},{_money(cost)}"
        )

    print()
    print(f"estimated_total_usd={_money(total_cost)}")
    print("pricing_note=estimate uses Gemini 2.5 Flash text rates: input $0.30/1M, output $2.50/1M")
    return 0


if __name__ == "__main__":
    sys.exit(main())
