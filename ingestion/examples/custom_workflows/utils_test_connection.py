"""
测试连接
"""

import json
import logging
import uuid
from functools import singledispatch

from utils import (
    get_service_by_metadata,
)

from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.generated.schema.entity.services.serviceType import ServiceType
from metadata.generated.schema.entity.services.connections.metadata import (
    openMetadataConnection
)
from metadata.generated.schema.entity.automations.workflow import (
    Workflow as AutomationWorkflow,
    WorkflowType,
)

from metadata.automations.runner import (
    execute,
    run_workflow,
)
from metadata.utils.secrets.secrets_manager_factory import SecretsManagerFactory
from metadata.generated.schema.entity.automations.testServiceConnection import (
    TestServiceConnectionRequest,
)
from metadata.ingestion.source.connections import get_connection, get_test_connection_fn


@singledispatch
def run_test_connection(*_, **__):
    raise NotImplementedError(f"Test connection not implemented!")


@run_test_connection.register
def _(openmetadata_connection: openMetadataConnection.OpenMetadataConnection, automation_workflow_name: str):
    # we need to instantiate the secret manager in case secrets are passed
    SecretsManagerFactory(
        openmetadata_connection.secretsManagerProvider,
        openmetadata_connection.secretsManagerLoader,
    )

    metadata = OpenMetadata(
        config=openmetadata_connection
    )
    automation_workflow = metadata.get_by_name(entity=AutomationWorkflow, fqn=automation_workflow_name)

    run_workflow(automation_workflow.request, automation_workflow, metadata)


@run_test_connection.register
def _(metadata: OpenMetadata, automation_workflow_name: str):
    automation_workflow = metadata.get_by_name(entity=AutomationWorkflow, fqn=automation_workflow_name)
    run_workflow(automation_workflow.request, automation_workflow, metadata)


@run_test_connection.register
def _(automation_workflow: AutomationWorkflow, generated: bool = False):
    """
    注意此种方式调用，需要元数据系统先配置测试连接的配置
    :param automation_workflow:
    :return:
    """
    # we need to instantiate the secret manager in case secrets are passed
    SecretsManagerFactory(
        automation_workflow.openMetadataServerConnection.secretsManagerProvider,
        automation_workflow.openMetadataServerConnection.secretsManagerLoader,
    )

    metadata = OpenMetadata(
        config=automation_workflow.openMetadataServerConnection
    )

    run_workflow(automation_workflow.request, automation_workflow if not generated else None, metadata)


def build_automation_workflow(openmetadata_connection: openMetadataConnection.OpenMetadataConnection,
                              service_type: ServiceType,
                              service_name: str,
                              ):
    metadata = OpenMetadata(
        config=openmetadata_connection
    )

    service = get_service_by_metadata(metadata, service_type, service_name)
    service_connection_type = service.connection.config.type.value

    automation_workflow = AutomationWorkflow(
        id=uuid.uuid1(),
        name=f"test_connection_{service_connection_type}_{service_name}",
        workflowType=WorkflowType.TEST_CONNECTION.value,
        openMetadataServerConnection=openmetadata_connection,
        request={
            "connection": service.connection,
            "serviceType": service_type,
            "serviceName": service_name,
            "connectionType": service_connection_type,
        }
    )

    return automation_workflow
