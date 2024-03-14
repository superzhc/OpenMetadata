import unittest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.event import listen
from sqlalchemy.pool import QueuePool

URL = "doris://root:@10.90.20.60:9030"


class DorisTest(unittest.TestCase):
    engine: Engine = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.engine = create_engine(
            URL,
            # connect_args=get_connection_args_fn(connection),
            poolclass=QueuePool,
            pool_reset_on_return=None,  # https://docs.sqlalchemy.org/en/14/core/pooling.html#reset-on-return
            echo=False,
            max_overflow=-1,
        )
