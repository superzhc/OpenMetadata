"""
测试连接
"""

import json
import logging
from functools import singledispatch

from metadata.ingestion.ometa.ometa_api import OpenMetadata
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
def _(automation_workflow: AutomationWorkflow):
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

    run_workflow(automation_workflow.request, automation_workflow, metadata)


@run_test_connection.register
def _(request: TestServiceConnectionRequest):
    # request_json = json.loads(request_str)
    # request: TestServiceConnectionRequest = TestServiceConnectionRequest.parse_obj(request_json)
    connection = get_connection(request.connection.config)

    test_connection_fn = get_test_connection_fn(request.connection.config)


if __name__ == "__main__":
    from metadata.utils.logger import set_loggers_level

    set_loggers_level(logging.DEBUG)

    automation_workflow_str = '''
    {
    "id": "747f8b45-1e7d-4ed7-9443-2287315a2136",
    "name": "test-connection-Http-oNtokqaN",
    "fullyQualifiedName": "test-connection-Http-oNtokqaN",
    "workflowType": "TEST_CONNECTION",
    "request": {
        "connection": {
            "config": {
                "type": "Http",
                "protocol": "HTTP",
                "hostPort": "www.baidu.com",
                "path": "/api",
                "method": "POST",
                "headers": {
                    "Accept": "*/*",
                    "Content-Type": "application/json"
                },
                "query": {
                    "param1": "val1",
                    "param2": "val2"
                },
                "body": "{\\\"d1\\\":\\\"bval1\\\",\\\"d2\\\":10}",
                "authorization":{
                    "type":"BasicAuth",
                    "config":{
                        "username":"zhangsan",
                        "password":"123456"
                    }
                }
            }
        },
        "serviceType": "Network",
        "connectionType": "Http",
        "secretsManagerProvider": "noop"
    },
    "openMetadataServerConnection": {
        "clusterName": "openmetadata",
        "type": "OpenMetadata",
        "hostPort": "http://10.90.20.236:8585/api",
        "authProvider": "openmetadata",
        "verifySSL": "no-ssl",
        "securityConfig": {
            "jwtToken": "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJlbWFpbCI6ImluZ2VzdGlvbi1ib3RAb3Blbm1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3MTMzMzI1OTEsImV4cCI6bnVsbH0.ETBNaooAgHfwO_o0qL10EV8BaQg1yxJ89fTOTh4w_E_u1Xte44AL994uZH5HbivSIcgvOlxKBclCz1zfFUwhh5KIpjO2GPCB4F_iq_bCOL8c3IVlFOqBdhbDcTPWqeTJwyhH9sH_-F4sXRlXtiYrBFNYnCcuDzzfJHVcTKgLbem-AQYVXHtm3ON0NLJecKzwKWMZydHc-0gYo6PtuN40OeL5ZcooHywr7gaewWKUnYW_6R6aMm1uwT1491QvDHm0hmA10UzKdjrMq4u3U4pU4tR4t5hj_OfNICdemfWOa1h4Yo51-7qSyRYDsJxVds5LPoEKHrP4NHiEsMmAAaHY3w"
        },
        "secretsManagerProvider": "noop",
        "secretsManagerLoader": "noop",
        "apiVersion": "v1",
        "includeTopics": true,
        "includeTables": true,
        "includeDashboards": true,
        "includePipelines": true,
        "includeMlModels": true,
        "includeUsers": true,
        "includeTeams": true,
        "includeGlossaryTerms": true,
        "includeTags": true,
        "includePolicy": true,
        "includeMessagingServices": true,
        "enableVersionValidation": true,
        "includeDatabaseServices": true,
        "includePipelineServices": true,
        "limitRecords": 1000,
        "forceEntityOverwriting": false,
        "storeServiceConnection": true,
        "supportsDataInsightExtraction": true,
        "supportsElasticSearchReindexingExtraction": true
    },
    "version": 0.1,
    "updatedAt": 1715242573154,
    "updatedBy": "hanyun-metadata",
    "href": "http://127.0.0.1:8585/api/v1/automations/workflows/747f8b45-1e7d-4ed7-9443-2287315a2136",
    "deleted": false
}
    '''
    # automation_workflow_json = json.loads(automation_workflow_str)
    # automation_workflow: AutomationWorkflow = AutomationWorkflow.parse_obj(automation_workflow_json)
    # run_test_connection(automation_workflow)

    from utils import build_openmetadataConnection

    openmetadataServerConnection: openMetadataConnection.OpenMetadataConnection = build_openmetadataConnection(
        host_port="http://10.90.20.236:8585/api",
        auth_config="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJlbWFpbCI6ImluZ2VzdGlvbi1ib3RAb3Blbm1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3MTMzMzI1OTEsImV4cCI6bnVsbH0.ETBNaooAgHfwO_o0qL10EV8BaQg1yxJ89fTOTh4w_E_u1Xte44AL994uZH5HbivSIcgvOlxKBclCz1zfFUwhh5KIpjO2GPCB4F_iq_bCOL8c3IVlFOqBdhbDcTPWqeTJwyhH9sH_-F4sXRlXtiYrBFNYnCcuDzzfJHVcTKgLbem-AQYVXHtm3ON0NLJecKzwKWMZydHc-0gYo6PtuN40OeL5ZcooHywr7gaewWKUnYW_6R6aMm1uwT1491QvDHm0hmA10UzKdjrMq4u3U4pU4tR4t5hj_OfNICdemfWOa1h4Yo51-7qSyRYDsJxVds5LPoEKHrP4NHiEsMmAAaHY3w",
    )

    # run_test_connection(openmetadataServerConnection, "test-connection-Redis-oNtokqaN")
    run_test_connection(openmetadataServerConnection, "test-connection-MQTT-dev")
