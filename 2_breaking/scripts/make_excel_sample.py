import csv
import sys
import time
from pathlib import Path


def tag_value(line: str) -> str:
    first_quote = line.find('"')
    last_quote = line.rfind('"')

    if first_quote == -1 or last_quote <= first_quote:
        return ""

    return line[first_quote + 1:last_quote]


def main() -> None:
    if len(sys.argv) != 4:
        print(
            "Usage: python make_excel_sample.py "
            "INPUT.pgn OUTPUT.csv NUMBER_OF_GAMES"
        )
        raise SystemExit(1)

    input_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2])
    limit = int(sys.argv[3])

    if not input_path.is_file():
        print(f"Input file not found: {input_path}")
        raise SystemExit(1)

    result = ""
    white_elo = ""
    black_elo = ""
    games_written = 0
    start = time.perf_counter()

    with (
        input_path.open(
            "r",
            encoding="utf-8",
            errors="replace",
        ) as source,
        output_path.open(
            "w",
            encoding="utf-8",
            newline="",
        ) as destination,
    ):
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

        for line in source:
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
                    result
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
                        print(
                            f"Wrote {games_written:,} games",
                            flush=True,
                        )

                    if games_written >= limit:
                        break

    elapsed = time.perf_counter() - start

    print()
    print(f"Games written: {games_written:,}")
    print(f"Total CSV lines: {games_written + 1:,}")
    print(f"Elapsed time: {elapsed:.2f} seconds")
    print(f"Output: {output_path.resolve()}")


if __name__ == "__main__":
    main()
