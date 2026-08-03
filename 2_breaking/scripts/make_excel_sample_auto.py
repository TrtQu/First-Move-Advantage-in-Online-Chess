#Similar to make_excel_sample.py.
import csv
import io
import sys
import time
from pathlib import Path

import zstandard as zstd


ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"


def tag_value(line: str) -> str:
    first = line.find('"')
    last = line.rfind('"')

    if first == -1 or last <= first:
        return ""

    return line[first + 1:last]


def open_text_stream(path: Path):
    raw_file = path.open("rb")
    magic = raw_file.read(4)
    raw_file.seek(0)

    if magic == ZSTD_MAGIC:
        print("Detected Zstandard-compressed input.", flush=True)

        decompressor = zstd.ZstdDecompressor()
        binary_stream = decompressor.stream_reader(raw_file)

        text_stream = io.TextIOWrapper(
            binary_stream,
            encoding="utf-8",
            errors="replace",
            newline="",
        )

        return raw_file, text_stream

    print("Detected uncompressed text input.", flush=True)

    text_stream = io.TextIOWrapper(
        raw_file,
        encoding="utf-8",
        errors="replace",
        newline="",
    )

    return raw_file, text_stream


def main() -> None:
    if len(sys.argv) != 4:
        print(
            "Usage: python make_excel_sample_auto.py "
            "INPUT_FILE OUTPUT.csv GAME_LIMIT"
        )
        raise SystemExit(1)

    input_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2])
    limit = int(sys.argv[3])

    if not input_path.is_file():
        print(f"Input not found: {input_path}")
        raise SystemExit(1)

    start = time.perf_counter()
    games_written = 0

    result = ""
    white_elo = ""
    black_elo = ""

    raw_file, source = open_text_stream(input_path)

    try:
        with output_path.open(
            "w",
            encoding="utf-8",
            newline="",
        ) as destination:
            writer = csv.writer(destination)

            writer.writerow(
                [
                    "game_number",
                    "result",
                    "white_elo",
                    "black_elo",
                    "average_elo",
                ]
            )

            for raw_line in source:
                line = raw_line.lstrip("\ufeff").strip()

                if line.startswith('[Event "'):
                    result = ""
                    white_elo = ""
                    black_elo = ""

                elif line.startswith('[Result "'):
                    result = tag_value(line)

                elif line.startswith('[WhiteElo "'):
                    white_elo = tag_value(line)

                elif line.startswith('[BlackElo "'):
                    black_elo = tag_value(line)

                    if (
                        result in {"1-0", "0-1", "1/2-1/2"}
                        and white_elo.isdigit()
                        and black_elo.isdigit()
                    ):
                        games_written += 1

                        average_elo = (
                            int(white_elo) + int(black_elo)
                        ) / 2

                        writer.writerow(
                            [
                                games_written,
                                result,
                                white_elo,
                                black_elo,
                                average_elo,
                            ]
                        )

                        if games_written % 100_000 == 0:
                            elapsed = time.perf_counter() - start

                            print(
                                f"Wrote {games_written:,} games "
                                f"in {elapsed:.1f} seconds",
                                flush=True,
                            )

                        if games_written >= limit:
                            break

    finally:
        try:
            source.close()
        except Exception:
            pass

        try:
            raw_file.close()
        except Exception:
            pass

    elapsed = time.perf_counter() - start

    print()
    print(f"Games written: {games_written:,}")
    print(f"Total CSV lines: {games_written + 1:,}")
    print(f"Elapsed time: {elapsed:.2f} seconds")
    print(f"Output: {output_path.resolve()}")

    if games_written == 0:
        print()
        print("ERROR: No valid PGN game records were detected.")
        raise SystemExit(2)


if __name__ == "__main__":
    main()
