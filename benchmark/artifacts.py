"""Execute a benchmark and upload its artifacts even on nonzero process exit."""
import argparse
import os
from pathlib import Path
import subprocess
import sys


def upload(directory, destination):
    from google.cloud import storage
    if not destination.startswith("gs://") or not destination.removeprefix("gs://").partition("/")[0]:
        raise ValueError("BENCH_GCS_PREFIX must be a gs://bucket[/prefix] URI")
    bucket_name, _, prefix = destination.removeprefix("gs://").partition("/")
    bucket = storage.Client().bucket(bucket_name)
    count = 0
    for path in Path(directory).rglob("*"):
        if path.is_file():
            name = "/".join(part for part in (prefix.rstrip("/"), str(path.relative_to(directory))) if part)
            bucket.blob(name).upload_from_filename(str(path))
            count += 1
    print(f"Uploaded {count} artifact(s) to {destination}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("Specify command after --")
    directory = os.getenv("BENCH_RESULTS", "result")
    Path(directory).mkdir(parents=True, exist_ok=True)
    result = subprocess.run(command, check=False)
    destination = os.getenv("BENCH_GCS_PREFIX")
    if destination:
        upload(directory, destination)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
