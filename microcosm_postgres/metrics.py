from enum import Enum

from microcosm.api import defaults, typed
from microcosm.config.types import boolean
from microcosm.errors import NotBoundError
from microcosm_logging.timing import elapsed_time


@defaults(
    enabled=typed(boolean, default_value=True)
)
class PostgresStoreMetrics:
    """
    Send metrics regarding the postgres store

    """

    def __init__(self, graph):
        self.metrics = self.get_metrics(graph)
        self.enabled = bool(
            self.metrics
            and self.metrics.host != "localhost"
            and graph.config.postgres_store_metrics.enabled
        )

    def get_metrics(self, graph):
        """
        Fetch the metrics client from the graph.

        Metrics will be disabled if the not configured.

        """
        try:
            return graph.metrics
        except NotBoundError:
            return None

    def __call__(self, model_name, action, elapsed_time, execution_result):
        """
        Send metrics for how long it takes to execute a sql operation

        """
        if not self.enabled:
            return

        if not model_name:
            return

        key = "store"
        tags = [
            "source:microcosm-postgres",
            f"result:{execution_result}",
            f"action:{action}",
            f"model_name:{model_name}",
        ]
        self.metrics.histogram(
            key,
            elapsed_time,
            tags=tags,
        )


class SQLExecutionStatus(Enum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


def postgres_metric_timing(action):
    def wrap(func):
        def func_wrapper(self, *args, **kwargs):
            extra = dict(
                model_name=self.model_name,
                action=action
            )
            try:
                with elapsed_time(extra):
                    try:
                        result = func(self, *args, **kwargs)
                        execution_status = SQLExecutionStatus.SUCCESS.name
                        return result
                    except Exception:
                        execution_status = SQLExecutionStatus.FAILURE.name
                        raise
            finally:
                self.postgres_store_metrics(
                    execution_result=execution_status,
                    **extra
                )

        return func_wrapper

    return wrap
