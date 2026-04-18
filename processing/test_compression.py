"""
Test PGN → Parquet compression ratio using a small Lichess sample.
Downloads ~50MB, parses, converts, measures compression.
"""

import io
import os
import time
import urllib.request
import zstandard as zstd
import chess.pgn
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

SAMPLE_URL = "https://database.lichess.org/standard/lichess_db_standard_rated_2024-01.pgn.zst"
SAMPLE_BYTES = 50 * 1024 * 1024  # download first 50MB only
OUTPUT_DIR = "processing/output"

os.makedirs(OUTPUT_DIR, exist_ok=True)


def download_sample(url: str, num_bytes: int) -> bytes:
    print(f"Downloading first {num_bytes // 1024 // 1024}MB from Lichess...")
    req = urllib.request.Request(url, headers={"Range": f"bytes=0-{num_bytes}"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = resp.read()
    print(f"  Downloaded: {len(data) / 1024 / 1024:.1f} MB compressed")
    return data


def decompress(data: bytes) -> str:
    print("Decompressing .zst...")
    dctx = zstd.ZstdDecompressor()
    # decompress what we can (partial file is ok for sampling)
    try:
        raw = dctx.decompress(data, max_output_size=500 * 1024 * 1024)
    except zstd.ZstdError:
        # partial decompression — stream instead
        raw = b""
        with dctx.stream_reader(io.BytesIO(data)) as reader:
            while True:
                chunk = reader.read(1024 * 1024)
                if not chunk:
                    break
                raw += chunk
    text = raw.decode("utf-8", errors="ignore")
    print(f"  Decompressed: {len(text) / 1024 / 1024:.1f} MB raw PGN")
    return text


def parse_pgn(text: str) -> list[dict]:
    print("Parsing PGN games...")
    games = []
    pgn_io = io.StringIO(text)

    while True:
        try:
            game = chess.pgn.read_game(pgn_io)
        except Exception:
            continue
        if game is None:
            break

        headers = game.headers
        moves = list(game.mainline_moves())

        games.append({
            "game_id":      headers.get("Site", "").split("/")[-1],
            "white":        headers.get("White", ""),
            "black":        headers.get("Black", ""),
            "white_elo":    int(headers.get("WhiteElo", 0) or 0),
            "black_elo":    int(headers.get("BlackElo", 0) or 0),
            "result":       headers.get("Result", ""),
            "time_control": headers.get("TimeControl", ""),
            "opening_eco":  headers.get("ECO", ""),
            "opening_name": headers.get("Opening", ""),
            "total_moves":  len(moves),
            "moves_uci":    " ".join(m.uci() for m in moves),
            "date":         headers.get("UTCDate", ""),
        })

        if len(games) % 1000 == 0:
            print(f"  Parsed {len(games)} games so far...")

    print(f"  Total games parsed: {len(games)}")
    return games


def measure_compression(games: list[dict]):
    df = pd.DataFrame(games)
    print(f"\nDataFrame shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")

    results = {}

    for compression in ["snappy", "zstd", "gzip"]:
        path = f"{OUTPUT_DIR}/sample_{compression}.parquet"
        pq.write_table(
            pa.Table.from_pandas(df),
            path,
            compression=compression
        )
        size_mb = os.path.getsize(path) / 1024 / 1024
        results[compression] = size_mb
        print(f"  Parquet ({compression}): {size_mb:.2f} MB")

    return results


def extrapolate(raw_pgn_mb: float, parquet_sizes: dict, full_file_gb: float = 30):
    print(f"\n{'='*50}")
    print(f"EXTRAPOLATION: {full_file_gb}GB compressed Lichess dump")
    print(f"{'='*50}")

    # rough ratio: compressed .zst PGN is ~6-8x smaller than raw PGN
    estimated_raw_pgn_gb = full_file_gb * 7
    print(f"Estimated raw PGN after decompression: ~{estimated_raw_pgn_gb:.0f} GB")

    for compression, sample_mb in parquet_sizes.items():
        ratio = raw_pgn_mb / sample_mb
        estimated_parquet_gb = estimated_raw_pgn_gb * 1024 / ratio / 1024
        print(f"  Parquet ({compression}): ~{estimated_parquet_gb:.1f} GB  (ratio {ratio:.1f}x)")


def main():
    start = time.time()

    raw_zst = download_sample(SAMPLE_URL, SAMPLE_BYTES)
    raw_pgn = decompress(raw_zst)
    raw_pgn_mb = len(raw_pgn) / 1024 / 1024

    games = parse_pgn(raw_pgn)
    if not games:
        print("No games parsed — sample too small or download failed")
        return

    parquet_sizes = measure_compression(games)

    extrapolate(raw_pgn_mb, parquet_sizes)

    print(f"\nDone in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
