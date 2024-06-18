from typing import List

from metadata.profiler.interface.sqlalchemy.profiler_interface import (
    SQAProfilerInterface,
)
from metadata.profiler.metrics.registry import Metrics
from metadata.profiler.processor.runner import QueryRunner
from metadata.utils.logger import profiler_interface_registry_logger
from metadata.utils.mysql_utils import (
    get_version,
)

logger = profiler_interface_registry_logger()


class MysqlProfilerInterface(SQAProfilerInterface):
    """
    Interface to interact with registry supporting
    sqlalchemy.
    """

    def _compute_window_metrics(
            self,
            metrics: List[Metrics],
            runner: QueryRunner,
            column,
            session,
            *args,
            **kwargs,
    ):
        """Given a list of metrics, compute the given results
        and returns the values

        Args:
            column: the column to compute the metrics against
            metrics: list of metrics to compute
        Returns:
            dictionnary of results
        """
        if get_version(self.session) >= 8:
            return super()._compute_window_metrics(metrics, runner, column, session, *args, **kwargs)

        return None
