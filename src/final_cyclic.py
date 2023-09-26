import typing
from datetime import datetime

from typing_extensions import Annotated

from flytekit import task, workflow
from flytekit.core.artifact import Artifact
from flytekit.types.file import FlyteFile

TarArtifact = Artifact(name="cyclic-tar", partitions={"ds": "{{ .inputs.as_of[%YYYY-%MM-%DD] }}"})
# Upstream = Artifact(name="cyclic-tar", partitions={"ds": "{{ .inputs.ds }}"})


@task
def update_tar(
    as_of: datetime, upstream_data: FlyteFile, file: typing.Optional[FlyteFile]
) -> Annotated[FlyteFile, TarArtifact]:
    ...


@workflow
def run_update_tar(
    as_of: datetime, upstream_data: FlyteFile, file: typing.Optional[FlyteFile] = TarArtifact.as_query()
):
    update_tar(as_of, upstream_data, file)


t1 = Trigger.on(daily_upstream=Artifact.query(name="upstream_data", partitions={"ds": "{{ .locals.foo }}"}),).run(
    run_update_tar,
    as_of=".trigger.locals.foo",  # cast string to datetime, what can do this?
    upstream_data=".trigger.items.daily_upstream",
    file=TarArtifact.as_query(partitions={"ds": "{{ .trigger.locals.foo - 1 }}"}),
    type_hints={
        "foo": datetime,
    },
)


"""
The dependency structure is like

         upstream_data::            upstream_data::              upstream_data::
           ds=2023-02-01              ds=2023-02-02                ds=2023-02-03
                |                          |                            |
                |                          |                            |
                ↓                          ↓                            ↓
               cyclic-tar::               cyclic-tar::                 cyclic-tar::
                  ds=2023-02-01              ds=2023-02-02                ds=2023-02-03
                     \ ____________________↗             \ ______________↗

If for whatever reason, we know that upstream_data for Feb. 1st and Feb. 2nd will be regenerated, we should
make sure upstream data gets updated for Feb. 1st first, otherwise you'll end up running 2/2 and 2/3 twice.
"""
