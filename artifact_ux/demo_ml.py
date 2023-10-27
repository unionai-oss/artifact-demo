import random
import typing
from datetime import datetime

import pandas as pd
from flytekit import CronSchedule, LaunchPlan
from flytekit.core.artifact import Artifact, Inputs
from flytekit.core.task import task
from flytekit.core.workflow import workflow
from flytekit.types.file import FlyteFile
from typing_extensions import Annotated


RideCountData = Artifact(
    name="ride_count_data",
    time_partitioned=True,
    partition_keys=["region"],
)


def get_permutations(s: str) -> typing.List[str]:
    if len(s) <= 1:
        return [s]
    permutations = []
    for i, char in enumerate(s):
        remaining_chars = s[:i] + s[i + 1 :]
        for perm in get_permutations(remaining_chars):
            permutations.append(char + perm)

    return permutations


@task
def gather_data(region: str, date: datetime) -> Annotated[pd.DataFrame, RideCountData.bind_time_partition(Inputs.date)(region=Inputs.region)]:
    """
    This task will produce a dataframe for a given region and date.  The dataframe will be stored in a well-known
    location, and will be versioned by the region and date.
    """
    print(f"Running gather data for region {region} and date {date}")
    fake_neighborhoods = get_permutations(region)
    fake_rides = [random.randint(100, 1000) for _ in range(len(fake_neighborhoods))]
    df = pd.DataFrame({"sectors": fake_neighborhoods, "rides": fake_rides})
    return df


@workflow
def run_gather_data(run_date: datetime):
    regions = ["SEA", "LAX", "SFO"]
    for region in regions:
        gather_data(region=region, date=run_date)


lp_gather_data = LaunchPlan.get_or_create(
    name="scheduled_gather_data_lp",
    workflow=run_gather_data,
    schedule=CronSchedule(
        schedule="* * * * *",  # Run every minute for the demo
        kickoff_time_input_arg="run_date",
    ),
)

# Note:
# Let's say this launch plan is run for 2023-03-01
# You should be able to get the output of the task via this URL
# flyte://project/domain/ride_count_data@<exec-id>
# To retrieve a specific partition, you can append params (note the format of the
# flyte://av0.1/project/domain/ride_count_data@<exec-id>?region=SEA&ds=23_03-7
# flyte://av0.1/project/domain/ride_count_data?region=SEA&ds=23_03-7 -> gets the latest one

# Example of what one would have to write in normal Flyte.
# @workflow
# def gather_data_and_run_model(region: str, date: datetime):
#     data = gather_data(region=region, date=date)
#     train_model(region=region, data=data)


# todo: update region input
ModelArtifact = Artifact(name="my-model", tags=["{{ .inputs.region }}"])
Model = Annotated[FlyteFile, ModelArtifact]


# Note:
# Using a file in place of an nn.Module for simplicity
# This model will be accessible at flyte://av0.1/project/domain/my-model@<exec-id>
# If you use flyte://av0.1/project/domain/my-model, you will get the latest (chronological) artifact.
# What's a tag? I think we should have both a notion of a tag and a version. A tag is a string that can move
# and point to different artifacts over time. A version is a fixed immutable field of the Artifact object.
# To access the latest Model for a given tag, you can use a url like this:
# flyte://av0.1/project/domain/my-model:SEA


# @task
# def train_model(region: str, data: pd.DataFrame) -> Model:
#     print(f"Training model for region {region} with data {data}")
#     return FlyteFile(__file__)


@task
def train_model(region: str, data: pd.DataFrame) -> FlyteFile:
    print(f"Training model for region {region} with data {data}")
    return FlyteFile(__file__)


# This query will return the latest artifact for a given region and date.
# Note:
# The ds here is templated from a different source.
data_query = RideCountData.query(region=Inputs.region, time_partition=Inputs.kickoff_time)


@workflow
def run_train_model(region: str, kickoff_time: datetime, data: pd.DataFrame = data_query) -> Model:
    return train_model(region=region, data=data)
