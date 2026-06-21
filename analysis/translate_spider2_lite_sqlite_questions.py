#!/usr/bin/env python3
"""Translate Spider 2.0-Lite local SQLite questions to Korean."""

from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from pathlib import Path


IN_JSONL = Path("data/spider2/spider2-lite.jsonl")
OUT_CACHE = Path("data/spider2/spider2_lite_sqlite_question_ko.json")


def load_local_rows() -> list[dict]:
    rows = []
    for line in IN_JSONL.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if str(row.get("instance_id", "")).startswith("local"):
            rows.append(row)
    return rows


def load_cache() -> dict[str, str]:
    if not OUT_CACHE.exists():
        return {}
    return json.loads(OUT_CACHE.read_text(encoding="utf-8"))


def save_cache(cache: dict[str, str]) -> None:
    OUT_CACHE.parent.mkdir(parents=True, exist_ok=True)
    OUT_CACHE.write_text(json.dumps(cache, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def translate_batch(texts: list[str], timeout: int = 30) -> list[str]:
    markers = [f"__SPLIT_{i:03d}__" for i in range(len(texts))]
    joined_parts = []
    for i, text in enumerate(texts):
        if i:
            joined_parts.append(markers[i])
        joined_parts.append(text)
    joined = "\n".join(joined_parts)
    url = (
        "https://translate.googleapis.com/translate_a/single"
        "?client=gtx&sl=en&tl=ko&dt=t&format=text&q="
        + urllib.parse.quote(joined)
    )
    with urllib.request.urlopen(url, timeout=timeout) as response:
        data = json.loads(response.read().decode("utf-8"))
    translated = "".join(part[0] for part in data[0])

    outputs = []
    remaining = translated
    for marker in markers[1:]:
        before, sep, after = remaining.partition(marker)
        if not sep:
            raise ValueError(f"split marker not found: {marker}")
        outputs.append(before.strip())
        remaining = after
    outputs.append(remaining.strip())
    return outputs


def main() -> None:
    rows = load_local_rows()
    cache = load_cache()
    missing = [row for row in rows if not cache.get(row["instance_id"])]
    print(f"total={len(rows)} cached={len(cache)} missing={len(missing)}")

    batch_size = 8
    for start in range(0, len(missing), batch_size):
        batch = missing[start : start + batch_size]
        try:
            translations = translate_batch([row["question"] for row in batch])
        except Exception as exc:
            print(f"failed batch starting {batch[0]['instance_id']}: {exc}")
            save_cache(cache)
            continue
        for row, translated in zip(batch, translations):
            cache[row["instance_id"]] = translated
            print(f"{len(cache)}/{len(rows)} {row['instance_id']}")
        save_cache(cache)
        time.sleep(0.3)

    save_cache(cache)
    print(f"wrote {OUT_CACHE}")


if __name__ == "__main__":
    main()
