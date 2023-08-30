
from typing_extensions import Annotated
from dataclasses import dataclass
from datetime import datetime

from flytekit import workflow, task, LaunchPlan, CronSchedule
from flytekit.core.artifact import Artifact

RidesArtifact = Artifact(name="rides_table", partitions={"ds": "{{ .inputs.ds }}"})

update_rides = HiveTask(
    query="sql/rides.sql",
    inputs={"ds": datetime},
    outputs={"result": Annotated[str, RidesArtifact]},
)


@workflow
def run_update_rides():
    return update_rides()


@workflow
def create_rides_report():
    ...

