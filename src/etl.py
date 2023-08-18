"""
This code is not yet running but should serve as inspiration for traditional ETL flows.
"""

from typing_extensions import Annotated
from dataclasses import dataclass

from flytekit import workflow
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
        incentives: IncentivesTable = Artifact(name="incentives_table", partitions={"ds": "{{ .inputs.report_date }}"}).as_query(),
        ab_test_results: ABTestResults = Artifact(name="ab-test-results", tags=["final"]).as_query(trigger_type="auto")
):
    create_tableau_report(report_date, rides, incentives, ab_test_results)





