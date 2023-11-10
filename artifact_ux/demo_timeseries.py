import hashlib
import os
import random
import tempfile
import typing
from datetime import datetime, timedelta

from flytekit import task, workflow
from flytekit.core.artifact import Artifact, Inputs
from flytekit.trigger import Trigger
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile
from typing_extensions import Annotated

TimeSeriesArtifact = Artifact(name="timeseries-hashed", time_partitioned=True)
Upstream = Artifact(name="upstream_data", time_partitioned=True)


@task
def create_directory(ds: datetime) -> FlyteDirectory:
    # Create a directory with some files in it
    d = tempfile.mktemp()
    # create a list of the numbers 1-100. randomly remove 8 of them.
    # The ones that remain create a file for each number and place them in the directory
    # the content of the file is just the number itself.
    numbers = list(range(1, 101))
    random.shuffle(numbers)
    numbers = numbers[:92]
    numbers.sort()
    os.mkdir(d)
    for number in numbers:
        with open(os.path.join(d, str(number) + ".txt"), "w") as f:
            f.write(str(number) + "\n")
    print(f"Created directory {d} with files {numbers} for date {ds}")

    return FlyteDirectory(d)


@workflow
def create_upstream_directory(ds: datetime) -> Annotated[FlyteDirectory, Upstream(time_partition=Inputs.ds)]:
    return create_directory(ds=ds)


@task
def update_hashes(
    as_of: datetime,
    upstream_data: FlyteDirectory,
    hashes_file: typing.Optional[FlyteFile],
) -> Annotated[FlyteFile, TimeSeriesArtifact(time_partition=Inputs.as_of)]:
    """
    This task takes a folder of upstream_data, and a file of hashes. When called, it will traverse the folder,
    hash all the files, remove the hashes that no longer exist, update the hashes that have changed, add the new files
    to the hash, and print out what was added, changed and removed.
    """
    # Create a set to store the paths of the files we encounter during traversal
    encountered_files = set()

    upstream_data.download()

    # Traverse the folder and collect encountered files
    for root, _, files in os.walk(upstream_data.path):
        for file in files:
            encountered_files.add(file)

    # Load existing hashes from the hashes file if it exists
    existing_files = {}
    if hashes_file is not None:
        with open(hashes_file, "r") as f:
            for line in f:
                if line == "\n":
                    continue
                parts = line.strip().split(" ", 1)
                if len(parts) == 2:
                    existing_files[parts[0]] = parts[1]
                else:
                    print(f"Skipping line {line} because it doesn't have a hash")

    removed_files = set()
    for existing_file, _ in existing_files.items():
        if existing_file not in encountered_files:
            print(f"Removing {existing_file} because it no longer exists")
            removed_files.add(existing_file)

    for removed_file in removed_files:
        del existing_files[removed_file]

    for encountered_file in encountered_files:
        if encountered_file not in existing_files:
            print(f"Adding {encountered_file} because it's new")
            upstream_file = os.path.join(upstream_data, encountered_file)
            with open(upstream_file, "rb") as f:
                file_hash = hashlib.sha256(f.read()).hexdigest()
            existing_files[encountered_file] = file_hash

    # Find new and removed files
    new_files = encountered_files - set(existing_files.keys())

    # Print out what was added and removed
    print(f"As of {as_of}:")
    print(f"Added files: {new_files}")
    print(f"Removed files: {removed_files}")

    # Write the new data
    temp_file = tempfile.mktemp()
    with open(temp_file, "w") as f:
        keys = [k for k in existing_files.keys()]
        keys.sort()
        for file_path in keys:
            f.write(f"{file_path} {existing_files[file_path]}\n")

    return FlyteFile(temp_file)


trigger_hashes = Trigger(
    trigger_on=[Upstream],
    inputs={
        "as_of": Upstream.time_partition,
        "upstream_data": Upstream,
        "hashes_file": TimeSeriesArtifact.query(
            time_partition=Upstream.time_partition - timedelta(days=1)
        ),
    },
)


@trigger_hashes
@workflow
def run_update_hashes(
    as_of: datetime,
    upstream_data: FlyteDirectory,
    hashes_file: typing.Optional[FlyteFile],
) -> FlyteFile:
    return update_hashes(
        as_of=as_of, upstream_data=upstream_data, hashes_file=hashes_file
    )


"""
The dependency structure is like

         upstream_data::            upstream_data::              upstream_data::
           ds=2023-02-01              ds=2023-02-02                ds=2023-02-03
                |                          |                            |
                |                          |                            |
                ↓                          ↓                            ↓
              timeseries-hashed::         timeseries-hashed::          timeseries-hashed::
                  ds=2023-02-01              ds=2023-02-02                ds=2023-02-03
                     \ ____________________↗             \ ______________↗

If for whatever reason, we know that upstream_data for Feb. 1st and Feb. 2nd will be regenerated, we should
make sure upstream data gets updated for Feb. 1st first, otherwise you'll end up running 2/2 and 2/3 twice.
"""

if __name__ == "__main__":
    upstream_sample = create_upstream_directory(ds=datetime(2023, 6, 15))
    print(upstream_sample)
    hash_file = run_update_hashes(
        as_of=datetime(2023, 6, 15), upstream_data=upstream_sample, hashes_file=None
    )
    print(hash_file)
