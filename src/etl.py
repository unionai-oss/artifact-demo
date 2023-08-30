"""
This code is not yet running but should serve as inspiration for traditional ETL flows.
"""

from typing_extensions import Annotated
from dataclasses import dataclass
from datetime import datetime

from flytekit import workflow, task, LaunchPlan, CronSchedule
from flytekit.core.artifact import Artifact


# Would need to write a simple Type Transformer
@dataclass
class Table(object):
    ...


RidesTable = Annotated[Table, Artifact(name="rides_table", partitions={"ds": "{{ .inputs.ds }}"})]
update_rides = HiveTask(
    query="sql/rides.sql",
    inputs={"ds": datetime},
    outputs={"result": RidesTable},
)
# @workflow -> run update_rides

IncentivesTable = Annotated[Table, Artifact(name="incentives_table", partitions={"ds": "{{ .inputs.ds }}"})]
update_incentives = HiveTask(
    query="sql/incentives.sql", inputs={"ds": datetime}, outputs={"result": IncentivesTable}
)
# @workflow -> run update_incentives

ABTestResults = Annotated[Table, Artifact(name="ab-test-results", tags=["{{ .inputs.stage }}"])]
compute_ab_test_performance = HiveTask(
    # as an example, stage can be "preliminary", "ready-for-use", "final"
    query="sql/abtests.sql", inputs={"stage": str, "ds": datetime}, outputs={"result": ABTestResults}
)


# @workflow -> run compute_ab_test_performance


@workflow
def run_bi_reporting(
        report_date: datetime,
        rides: RidesTable = Artifact(name="rides_table", partitions={"ds": "{{ .inputs.report_date }}"}).as_query(),
        incentives: IncentivesTable = Artifact(name="incentives_table",
                                               partitions={"ds": "{{ .inputs.report_date }}"}).as_query(),
        ab_test_results: ABTestResults = Artifact(name="ab-test-results", tags=["final"]).as_query(trigger_type="auto")
):
    create_tableau_report(report_date, rides, incentives, ab_test_results)


# An example of why arbitrary partitions makes triggering hard
@task
def produce_upstream() -> int:
    ...


@workflow
def run_upstreamer() -> Annotated[int, Artifact(name="upstream_blah", partitions={"ds": "{{ .input.date }}"})]:
    return produce_upstream()


@workflow
def run_upstreamer_2() -> Annotated[int, Artifact(name="upstream_blah", partitions={"ds": "{{ .input.date }}"})]:
    return produce_upstream()


@workflow
def downstream(
        daily_upstream: int = Artifact.query(name="upstream_blah", partitions={"ds": "{{ .kick_off.date }}"}),
        daily_upstream2: int = Artifact.query(name="upstream_blah2", partitions={"ds": "{{ .kick_off.date }}"}),
        weekly_upstream: int = Artifact.query(name="upstream_weekly", partitions={"ds": "{{ .kick_off.date }}"}),
        region: str,
        model: int = Artifact.query(name="model", partitions={"region": "{{ .inputs.region }}"}),
        data: int = Artifact.query(name="data", partitions={"region": "{{ .inputs.region }}"}),
) -> Annotated[int, Artifact(name="downstream_blah", partitions={"ds": "{{ .input.date }}"})]:
    print(daily_upstream, weekly_upstream)



p_daily = DailyPartition("2/1/2023", "2/28/2023")

#                                                                                 "2/1/2023"
t1 = Trigger.on(
    daily_upstream=Artifact.query(name="upstream_blah", partitions={"ds": "{{ .locals.foo }}"}),
    daily_upstream2=Artifact.query(name="upstream_blah2", partitions={"ds": "{{ .locals.foo }}"}),
).run(
    downstream,
    daily_upstream=trigger.daily_upstream,
    daily_upstream2=trigger.daily_upstream2,
    weekly_upstream=Artifact.query(name="upstream_weekly", partitions={"ds": "{{ .trigger.locals.foo.week - 1 }}"}),
    region="SEA"
)

# t2 = Trigger.on(
#     daily_upstream2=Artifact.query(name="upstream_blah2", partitions={"ds": "{{ .event_day }}"}),
# ).run(
#     downstream2,
#     ...
# )

Event(artifact_created, name="upstream_blah", partitions={"ds": "02012023"})
Event(artifact_created, name="upstream_blah2", partitions={"ds": "02012023"})
