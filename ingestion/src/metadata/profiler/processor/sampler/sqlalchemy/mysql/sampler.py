from sqlalchemy.orm import Query

from metadata.generated.schema.entity.data.table import (
    ProfileSampleType,
)
from metadata.profiler.orm.functions.modulo import ModuloFn
from metadata.profiler.orm.functions.random_num import RandomNumFn
from metadata.profiler.processor.sampler.sqlalchemy.sampler import SQASampler
from metadata.utils.mysql_utils import (
    get_version,
)

RANDOM_LABEL = "random"


class MysqlSampler(SQASampler):
    """
    Mysql5.7 版本不支持CTE语法，改用子查询方式
    """

    def get_sample_query(self) -> Query:
        if get_version(self.client) >= 8:
            return super().get_sample_query()

        """get query for sample data"""
        if self.profile_sample_type == ProfileSampleType.PERCENTAGE:
            rnd = (
                self._base_sample_query(
                    (ModuloFn(RandomNumFn(), 100)).label(RANDOM_LABEL),
                )
                .subquery()
            )
            session_query = self.client.query(rnd)
            return session_query.where(rnd.c.random <= self.profile_sample).subquery()

        table_query = self.client.query(self.table)
        session_query = self._base_sample_query(
            (ModuloFn(RandomNumFn(), table_query.count())).label(RANDOM_LABEL),
        )
        return (
            session_query.order_by(RANDOM_LABEL)
            .limit(self.profile_sample)
            .subquery()
        )
