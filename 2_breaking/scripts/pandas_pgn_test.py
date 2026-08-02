import argparse
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import psutil


def run_pandas_child(pgn_path: Path) -> int:
    """Attempt to load the complete PGN into one pandas DataFrame."""
    start = time.perf_counter()

    print("Starting pandas.read_csv() without chunksize...", flush=True)
    print(f"Input: {pgn_path.resolve()}", flush=True)
    print(
        f"Raw file size: {pgn_path.stat().st_size / (1024 ** 3):.3f} GiB",
        flush=True,
    )

    try:
        # Control-A is extremely unlikely to occur in PGN data.
        # Therefore, every physical PGN line becomes one DataFrame row.
        dataframe = pd.read_csv(
            pgn_path,
            sep="\x01",
            header=None,
            names=["raw_pgn_line"],
            dtype=str,
            keep_default_na=False,
            engine="c",
        )

    except MemoryError:
        elapsed = time.perf_counter() - start
        print(f"MemoryError after {elapsed:.2f} seconds", flush=True)
        return 2

    except Exception as error:
        elapsed = time.perf_counter() - start
        print(
            f"{type(error).__name__} after {elapsed:.2f} seconds: {error}",
            flush=True,
        )
        return 3

    elapsed = time.perf_counter() - start

    dataframe_memory = int(
        dataframe.memory_usage(index=True, deep=True).sum()
    )

    print(f"Completed in {elapsed:.2f} seconds", flush=True)
    print(f"Rows loaded: {len(dataframe):,}", flush=True)
    print(
        f"DataFrame memory: "
        f"{dataframe_memory / (1024 ** 3):.3f} GiB",
        flush=True,
    )

    return 0


def monitor_test(pgn_path: Path, timeout_seconds: int) -> int:
    output_path = Path("pandas_output.txt")
    error_path = Path("pandas_error.txt")
    memory_log_path = Path("pandas_memory_log.txt")
    summary_path = Path("pandas_summary.txt")

    file_size = pgn_path.stat().st_size
    start = time.perf_counter()
    peak_rss = 0

    with (
        output_path.open("w", encoding="utf-8", buffering=1) as output_file,
        error_path.open("w", encoding="utf-8", buffering=1) as error_file,
        memory_log_path.open("w", encoding="utf-8", buffering=1) as memory_log,
    ):
        child = subprocess.Popen(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                str(pgn_path),
                "--child",
            ],
            stdout=output_file,
            stderr=error_file,
        )

        child_process = psutil.Process(child.pid)
        timed_out = False

        print("Pandas breaking-point experiment")
        print(f"Input: {pgn_path.resolve()}")
        print(f"Raw file size: {file_size / (1024 ** 3):.3f} GiB")
        print(f"Time limit: {timeout_seconds} seconds")
        print(f"Pandas process ID: {child.pid}")
        print()
        print("Open Task Manager now and watch the Python process.")
        print()

        while child.poll() is None:
            elapsed = time.perf_counter() - start

            if elapsed >= timeout_seconds:
                timed_out = True
                print(
                    f"Time limit reached after {elapsed:.2f} seconds. "
                    "Stopping pandas..."
                )

                child.terminate()

                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()

                break

            try:
                rss = child_process.memory_info().rss
                peak_rss = max(peak_rss, rss)
            except psutil.Error:
                rss = 0

            system_memory = psutil.virtual_memory()

            line = (
                f"elapsed={elapsed:.1f}s "
                f"pandas_rss={rss / (1024 ** 3):.3f} GiB "
                f"system_used={system_memory.used / (1024 ** 3):.3f} GiB "
                f"system_available="
                f"{system_memory.available / (1024 ** 3):.3f} GiB "
                f"memory_percent={system_memory.percent:.1f}%"
            )

            print(line, flush=True)
            memory_log.write(line + "\n")

            time.sleep(5)

        return_code = child.poll()
        elapsed = time.perf_counter() - start

    if timed_out:
        status = "TIME_LIMIT"
        exit_code = 124
    elif return_code == 0:
        status = "COMPLETED"
        exit_code = 0
    elif return_code == 2:
        status = "MEMORY_ERROR"
        exit_code = 2
    elif return_code is not None and return_code < 0:
        status = "PROCESS_KILLED"
        exit_code = return_code
    else:
        status = "ERROR"
        exit_code = return_code if return_code is not None else 3

    summary = (
        f"status={status}\n"
        f"exit_code={exit_code}\n"
        f"elapsed_seconds={elapsed:.2f}\n"
        f"raw_file_size_gib={file_size / (1024 ** 3):.3f}\n"
        f"peak_pandas_rss_gib={peak_rss / (1024 ** 3):.3f}\n"
    )

    summary_path.write_text(summary, encoding="utf-8")

    print()
    print("FINAL SUMMARY")
    print(summary)
    print(f"Detailed pandas output: {output_path.resolve()}")
    print(f"Error log: {error_path.resolve()}")
    print(f"Memory log: {memory_log_path.resolve()}")
    print(f"Summary: {summary_path.resolve()}")

    return exit_code


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("pgn_file", type=Path)
    parser.add_argument("--timeout", type=int, default=720)
    parser.add_argument("--child", action="store_true")
    args = parser.parse_args()

    if not args.pgn_file.is_file():
        print(f"File not found: {args.pgn_file}")
        return 1

    if args.child:
        return run_pandas_child(args.pgn_file)

    return monitor_test(args.pgn_file, args.timeout)


if __name__ == "__main__":
    raise SystemExit(main())
