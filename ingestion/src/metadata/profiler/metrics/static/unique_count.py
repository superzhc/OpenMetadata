#  Copyright 2021 Collate
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""
Unique Count Metric definition
"""
from typing import Optional

from sqlalchemy import column, func
from sqlalchemy.orm import DeclarativeMeta, Session

from metadata.profiler.metrics.core import QueryMetric
from metadata.profiler.orm.functions.unique_count import _unique_count_query_mapper
from metadata.profiler.orm.registry import NOT_COMPUTE
from metadata.utils.logger import profiler_logger

logger = profiler_logger()


class UniqueCount(QueryMetric):
    """
    UNIQUE_COUNT Metric

    Given a column, count the number of values appearing only once
    """

    @classmethod
    def name(cls):
        return "uniqueCount"

    @property
    def metric_type(self):
        return int

    def query(
        self, sample: Optional[DeclarativeMeta], session: Optional[Session] = None
    ):
        """
        Build the Unique Count metric
        """
        if not session:
            raise AttributeError(
                "We are missing the session attribute to compute the UniqueCount."
            )

        if self.col.type.__class__.__name__ in NOT_COMPUTE:
            return None

        # Run all queries on top of the sampled data
        col = column(self.col.name, self.col.type)
        unique_count_query = _unique_count_query_mapper[session.bind.dialect.name](
            col, session, sample
        )

        # 2024年6月12日 有必要还要定义为 CTE，直接使用
        # only_once_cte = unique_count_query.cte("only_once")
        # return session.query(func.count().label(self.name())).select_from(only_once_cte)
        return session.query(func.count().label(self.name())).select_from(unique_count_query)

    def df_fn(self, dfs=None):
        """
        Build the Unique Count metric
        """
        from collections import Counter  # pylint: disable=import-outside-toplevel

        try:
            counter = Counter()
            for df in dfs:
                df_col_value = df[self.col.name].dropna().to_list()
                counter.update(df_col_value)
            return len([key for key, value in counter.items() if value == 1])
        except Exception as err:
            logger.debug(
                f"Don't know how to process type {self.col.type}"
                f" when computing Unique Count.\n Error: {err}"
            )
            return 0
