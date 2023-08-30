from typing_extensions import Annotated

from flytekit import workflow
from flytekit.core.artifact import Artifact

# Somehow these are produced
DailyUpstream = Artifact(name="daily_upstream", partitions={"ds": "{{ .inputs.date }}"})
SecondDailyUpstream = Artifact(name="daily_upstream_2", partitions={"ds": "{{ .inputs.date }}"})
HourlyUpstream = Artifact(name="hourly_upstream", partitions={"ds": "{{ .inputs.datetime }}"})
Model = Artifact(name="model", partitions={"region": "{{ .inputs.region }}"})


Downstream = Artifact(name="downstream", partitions={"ds": "{{ .inputs.date }}"})


@workflow
def downstream(
    region: str,
    daily_upstream: int = Artifact.query(name="daily_upstream", partitions={"ds": "{{ .globals.kickoff_date }}"}),
    second_daily_upstream: int = Artifact.query(
        name="daily_upstream_2", partitions={"ds": "{{ .globals.kickoff_date }}"}
    ),
    hourly_upstream: int = Artifact.query(name="hourly_upstream", partitions={"ds": "{{ .globals.kickoff_hour }}"}),
    model: int = Artifact.query(name="model", partitions={"region": "{{ .inputs.region }}"}),
) -> Annotated[int, Downstream]:
    ...


t1 = Trigger.on(
    daily_upstream=Artifact.query(name="daily_upstream", partitions={"ds": "{{ .locals.foo }}"}),
    second_daily_upstream=Artifact.query(name="daily_upstream_2", partitions={"ds": "{{ .locals.foo }}"}),
).run(
    downstream,
    daily_upstream=".trigger.items.daily_upstream",
    second_daily_upstream=".trigger.items.second_daily_upstream",
    hourly_upstream=Artifact.query(name="hourly_upstream", partitions={"ds": "{{ .globals.kickoff_hour }}"}),
    region="SEA",
)

t2 = Trigger.on(hourly_upstream=Artifact.query(name="hourly_upstream", partitions={"ds": "{{ .locals.foo }}"}),).run(
    downstream,
    daily_upstream=Artifact.query(name="daily_upstream", partitions={"ds": "{{ .trigger.locals.foo[%YYYY-%MM-%DD] }}"}),
    second_daily_upstream=Artifact.query(
        # existence of the format field means that we should try to interpret this as a datetime
        name="daily_upstream_2", partitions={"ds": "{{ .trigger.locals.foo[%YYYY-%MM-%DD] }}"}
    ),
    region="SEA",
)
