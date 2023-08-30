from typing_extensions import Annotated
from datetime import datetime

from flytekit import workflow, task, LaunchPlan, CronSchedule
from flytekit.types.file import FlyteFile
from flytekit.core.artifact import Artifact

RidesArtifact = Artifact(name="rides_table", partitions={"ds": "{{ .inputs.ds }}"})

update_rides = HiveTask(
    query="sql/rides.sql",
    inputs={"ds": datetime},
    outputs={"result": str},
)


@workflow
def run_update_rides(ds: datetime) -> Annotated[str, RidesArtifact]:
    return update_rides(ds=ds)


Report = Artifact(name="rides_report", partitions={"ds": "{{ .inputs.ds }}"})


@workflow
def create_rides_report(a: str, ds: str) -> Annotated[FlyteFile, Report]:
    ...


Trigger.on(
    rides=RidesArtifact.as_query(partitions={"ds": "{{ .locals.foo }}"}),
).run(
    create_rides_report,
    a=".trigger.items.rides",
    ds=".trigger.locals.foo",
)

# This is registered
# this runs for 2/1 and 2/2 and 2/3
# Then code is updated, removing ds
# Report = Artifact(name="rides_report")
# @workflow
# def create_rides_report(a: str):
#     ...
#
#
# Trigger.on(
#     rides=RidesArtifact.as_query(partitions={"ds": "{{ .locals._ }}"}),
# ).run(
#     create_rides_report,
#     a=".trigger.items.rides",
# )
# then 2/1 data is updated.
# Options for back-fill behavior:
# 1. The trigger does nothing, because this is not a "new" artifact, it's an update of an existing partition.
# 2. The trigger does run, and just runs the new code.
# 3. If 1. users can manually detect the tree of executions, select a set of nodes from the tree and re-run
#      - the same older versions of launch plans are used, .on() Artifacts are passed in, queries are re-run.
# 4. If 1. or 2 we can show the current tree and show what would happen if a backfill plan is submitted.
