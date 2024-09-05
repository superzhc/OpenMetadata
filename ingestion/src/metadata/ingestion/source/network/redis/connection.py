from typing import Optional, Any
import redis
from metadata.generated.schema.entity.automations.workflow import (
    Workflow as AutomationWorkflow,
)
from metadata.generated.schema.entity.services.connections.network.redisConnection import (
    RedisConnection, RedisProtocolVersion
)
from metadata.ingestion.connections.test_connections import test_connection_steps
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


def get_connection(
        connection: RedisConnection
) -> redis.ConnectionPool:
    if RedisProtocolVersion.RESP3 == connection.protocol:
        protocol = 3
    else:
        protocol = 2

    pool = redis.ConnectionPool(
        host=connection.host,
        port=connection.port,
        db=connection.db,
        protocol=protocol,
        username=connection.username,
        password=connection.password.get_secret_value(),
    )
    return pool


def test_connection(
        metadata: OpenMetadata,
        client: redis.ConnectionPool,
        service_connection: RedisConnection,
        automation_workflow: Optional[AutomationWorkflow] = None,
) -> None:
    """
    Test connection. This can be executed either as part
    of a metadata workflow or during an Automation Workflow
    """

    def custom_executor():
        r = redis.Redis(connection_pool=client)
        r.ping()

    test_fn = {"CheckAccess": custom_executor}

    test_connection_steps(
        metadata=metadata,
        test_fn=test_fn,
        service_type=service_connection.type.value,
        automation_workflow=automation_workflow,
    )
