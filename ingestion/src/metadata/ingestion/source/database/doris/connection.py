"""
Source connection handler
"""
from typing import Optional

from sqlalchemy.engine import Engine

from metadata.generated.schema.entity.automations.workflow import (
    Workflow as AutomationWorkflow,
)
from metadata.generated.schema.entity.services.connections.database.dorisConnection import (
    DorisConnection,
)
from metadata.ingestion.connections.builders import (
    create_generic_db_connection,
    get_connection_args_common,
    get_connection_url_common,
    init_empty_connection_options,
)
from metadata.ingestion.connections.test_connections import (
    test_connection_db_schema_sources,
)
from metadata.ingestion.ometa.ometa_api import OpenMetadata


def get_connection(connection: DorisConnection) -> Engine:
    """
    Create connection
    """
    if connection.sslCA or connection.sslCert or connection.sslKey:
        if not connection.connectionOptions:
            connection.connectionOptions = init_empty_connection_options()
        if connection.sslCA:
            connection.connectionOptions.__root__["ssl_ca"] = connection.sslCA
        if connection.sslCert:
            connection.connectionOptions.__root__["ssl_cert"] = connection.sslCert
        if connection.sslKey:
            connection.connectionOptions.__root__["ssl_key"] = connection.sslKey
    return create_generic_db_connection(
        connection=connection,
        get_connection_url_fn=get_connection_url_common,
        get_connection_args_fn=get_connection_args_common,
    )


def test_connection(
        metadata: OpenMetadata,
        engine: Engine,
        service_connection: DorisConnection,
        automation_workflow: Optional[AutomationWorkflow] = None,
) -> None:
    """
    Test connection. This can be executed either as part
    of a metadata workflow or during an Automation Workflow
    """
    test_connection_db_schema_sources(
        metadata=metadata,
        engine=engine,
        service_connection=service_connection,
        automation_workflow=automation_workflow,
    )