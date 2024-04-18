import time
import json
import uuid
from typing import Union
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.generated.schema.entity.type import (
    entityReference
)
from metadata.generated.schema.metadataIngestion import (
    workflow
)
from metadata.generated.schema.entity.services.serviceType import ServiceType
from metadata.generated.schema.entity.services.connections.metadata import (
    openMetadataConnection
)
from metadata.generated.schema.entity.services.ingestionPipelines.ingestionPipeline import (
    IngestionPipeline,
    PipelineType,
)
from metadata.generated.schema.entity.services.dashboardService import DashboardService
from metadata.generated.schema.entity.services.databaseService import DatabaseService
from metadata.generated.schema.entity.services.messagingService import MessagingService
from metadata.generated.schema.entity.services.metadataService import MetadataService
from metadata.generated.schema.entity.services.mlmodelService import MlModelService
from metadata.generated.schema.entity.services.pipelineService import PipelineService
from metadata.generated.schema.entity.services.searchService import SearchService
from metadata.generated.schema.entity.services.storageService import StorageService
from openmetadata_managed_apis.workflows.ingestion.common import (
    metadata_ingestion_workflow,
)
from openmetadata_managed_apis.workflows.ingestion.metadata import (
    build_metadata_workflow_config,
)
from openmetadata_managed_apis.workflows.ingestion.profiler import (
    build_profiler_workflow_config,
    profiler_workflow,
)
from openmetadata_managed_apis.workflows.ingestion.test_suite import (
    build_test_suite_workflow_config,
    test_suite_workflow,
)
from openmetadata_managed_apis.workflows.ingestion.usage import (
    build_usage_workflow_config,
    usage_workflow,
)
from openmetadata_managed_apis.workflows.ingestion.lineage import (
    build_lineage_workflow_config,
)
from metadata.generated.schema.entity.automations.workflow import (
    Workflow as AutomationWorkflow,
    WorkflowType,
)
from metadata.generated.schema.entity.automations import testServiceConnection
from metadata.automations.runner import (
    execute,
    run_workflow,
)
from metadata.utils.secrets.secrets_manager_factory import SecretsManagerFactory

TOKEN = "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6IkFsbEF1dGgiLCJlbWFpbCI6IkFsbEF1dGhAeGdpdC5jb20iLCJpc0JvdCI6dHJ1ZSwidG9rZW5UeXBlIjoiQk9UIiwiaWF0IjoxNzA4NDk2MTExLCJleHAiOm51bGx9.GRsZtM6i-ty1w71wg6HOxFmYQHDFXDH278x-G861jmaxuDLSCiIjRE0UiVlbNlsKQXoDbQIAT20eehfjgJ2Bp2HwUYBF8obunkTAkPv6WACJ741y5PQ-a59AWBYQlNhYviKxlUtneqDbQs88wN0iJL8FR_pdaMhYzLraSZs1FdA6mC_bLjfLyU6aSHrnmh7C6vEYBZS4NKDXmAp9iqp_upm_p0bmk7KUbY540TbA7ilcAAvYoZtxfYp86irtTOEq-yVni2J37XMLyVqNy3cVFT9ZcNS5sPDsTFB011fqKW-aaSFM45nXGt5A0eYEHaBrKLM_kUrdt-WRL2gjIXDpKg"
TOKEN_249 = "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6Imhhbnl1bi1tZXRhZGF0YSIsImVtYWlsIjoiaGFueXVuLW1ldGFkYXRhQG9wZW5tZXRhZGF0YS5vcmciLCJpc0JvdCI6dHJ1ZSwidG9rZW5UeXBlIjoiQk9UIiwiaWF0IjoxNzExNDUyNDIzLCJleHAiOm51bGx9.G2Y5iFXaln0Li5TdsW6Xv9iEl-t9KHlpU_98rGc3y_9eSJnGIb3pPbeylIvF-PsW_L9LokuGAvZiCY9f0fCik9t3PQjsJxYiUA64iJPNJkr-zOA5AfmipqwMamNe-SrBdGVlk-p4XtLahTAc3tK3jVz-rCB3P28q1lxIoOy6yn2hEMrqUWJqc1HfeCispeAe8GRI7cy9hhj_3uetIZW1uaYvVtMPBZ4K4LMRWED9Hm3CcvNoNRIMzBVyLAmorpKO_iewlYH9FZBypc4vCAxv5LN9dQghsz0hry2rXr03KhAPUtF7wWARj03QoS6dJzirSXqvc9TomR8A48BRma9hTQ"

SERVICE_TYPE_CLASS_MAP = {
    ServiceType.Database: DatabaseService,
    ServiceType.Messaging: MessagingService,
    ServiceType.Metadata: MetadataService,
}


def build_openmetadata_server_connection(host_port: str, token: str,
                                         api_version: str = "v1",
                                         auth_provider: str = "openmetadata",
                                         ) -> dict:
    return build_openmetadataConnection(**{
        "api_version": api_version,
        "auth_provider": auth_provider,
        "host_port": host_port,
        "auth_config": token,
    }).dict()


def build_openmetadataConnection(host_port: str,
                                 api_version: str = "v1",
                                 auth_type: str = "JWT",
                                 auth_config=None,
                                 **kwargs
                                 ) -> openMetadataConnection.OpenMetadataConnection:
    connection_map = {
        "hostPort": host_port,
        "apiVersion": api_version,
        **kwargs
    }

    if "JWT" == auth_type:
        connection_map["authProvider"] = "openmetadata"
        connection_map["securityConfig"] = {
            "jwtToken": auth_config if auth_config else kwargs["token"],
        }
    else:
        connection_map["authProvider"] = auth_type
        connection_map["securityConfig"] = auth_config

    openMetadataServerConnection = openMetadataConnection.OpenMetadataConnection(**connection_map)

    # we need to instantiate the secret manager in case secrets are passed
    SecretsManagerFactory(
        openMetadataServerConnection.secretsManagerProvider,
        openMetadataServerConnection.secretsManagerLoader,
    )

    return openMetadataServerConnection


def standard_openmetadata_connection(
        openmetadata_server_connection: Union[dict, openMetadataConnection.OpenMetadataConnection]):
    return openmetadata_server_connection if isinstance(openmetadata_server_connection,
                                                        openMetadataConnection.OpenMetadataConnection) else openMetadataConnection.OpenMetadataConnection(
        **openmetadata_server_connection)


def build_request(openmetadata_server_connection: Union[dict, openMetadataConnection.OpenMetadataConnection],
                  service_type: ServiceType,
                  service_name: str
                  ):
    metadata = OpenMetadata(standard_openmetadata_connection(openmetadata_server_connection))

    service = metadata.get_by_name(
        entity=SERVICE_TYPE_CLASS_MAP.get(service_type),
        fqn=service_name,
        fields="*",
        nullable=False,
    )

    request = {
        "connection": service.connection,
        "connectionType": service.serviceType.value,
        # "secretsManagerProvider": "noop",
        "serviceName": service.name,
        "serviceType": service_type.value,
    }

    return request


def build_automation_workflow(openmetadata_server_connection: dict,
                              service_type: ServiceType,
                              service_name: str,
                              ):
    automation_workflow = AutomationWorkflow(
        id=uuid.uuid1(),
        name="test-connection-{}-{}".format(service_type.value, time.time()),
        workflowType=WorkflowType.TEST_CONNECTION,
        request=build_request(openmetadata_server_connection, service_type, service_name),
        openMetadataServerConnection=openmetadata_server_connection,
    )
    return automation_workflow


def run_test_connection_by_config(automation_workflow: AutomationWorkflow):
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

    execute(automation_workflow)


def run_test_connection(openmetadata_server_connection: Union[dict, openMetadataConnection.OpenMetadataConnection],
                        service_type: ServiceType,
                        service_name: str,
                        ):
    openMetadataServerConnection: openMetadataConnection.OpenMetadataConnection = openmetadata_server_connection if isinstance(
        openmetadata_server_connection,
        openMetadataConnection.OpenMetadataConnection) else openMetadataConnection.OpenMetadataConnection(
        **openmetadata_server_connection)

    # we need to instantiate the secret manager in case secrets are passed
    SecretsManagerFactory(
        openMetadataServerConnection.secretsManagerProvider,
        openMetadataServerConnection.secretsManagerLoader,
    )

    metadata = OpenMetadata(openMetadataServerConnection)

    service = metadata.get_by_name(
        entity=SERVICE_TYPE_CLASS_MAP.get(service_type),
        fqn=service_name,
        fields="*",
        nullable=False,
    )

    request = testServiceConnection.TestServiceConnectionRequest(**{
        "connection": service.connection,
        "connectionType": service.serviceType.value,
        "serviceName": service.name,
        "serviceType": service_type.value,
    })

    run_workflow(request, None, metadata)


def get_service_by_metadata(metadata: OpenMetadata,
                            service_type: ServiceType,
                            service_name: str,
                            ):
    service = metadata.get_by_name(
        entity=SERVICE_TYPE_CLASS_MAP.get(service_type),
        fqn=service_name,
        fields="*",
        nullable=False,
    )
    return service


def run_metadata_database(openmetadataServerConnection: openMetadataConnection.OpenMetadataConnection,
                          service_name: str,
                          logger_level: workflow.LogLevels = workflow.LogLevels.DEBUG,
                          **kwargs
                          ):
    # from metadata.generated.schema.metadataIngestion.databaseServiceMetadataPipeline import (
    #     DatabaseServiceMetadataPipeline,
    # )
    #
    # source_config = DatabaseServiceMetadataPipeline(**kwargs)
    run_metadata(openmetadataServerConnection=openmetadataServerConnection,
                 service_type=ServiceType.Database,
                 service_name=service_name,
                 logger_level=logger_level,
                 # **source_config.dict()
                 **kwargs
                 )


def run_metadata_messaging(openmetadataServerConnection: openMetadataConnection.OpenMetadataConnection,
                           service_name: str,
                           logger_level: workflow.LogLevels = workflow.LogLevels.DEBUG,
                           **kwargs
                           ):
    run_metadata(openmetadataServerConnection=openmetadataServerConnection,
                 service_type=ServiceType.Messaging,
                 service_name=service_name,
                 logger_level=logger_level,
                 **kwargs
                 )


def run_metadata(openmetadataServerConnection: openMetadataConnection.OpenMetadataConnection,
                 service_type: ServiceType,
                 service_name: str,
                 logger_level: workflow.LogLevels = workflow.LogLevels.DEBUG,
                 **source_config
                 ):
    run_by_custom(openmetadataServerConnection=openmetadataServerConnection,
                  service_type=service_type,
                  service_name=service_name,
                  pipeline_type="metadata",
                  logger_level=logger_level,
                  **source_config
                  )


def run_profiler_database(openmetadataServerConnection: openMetadataConnection.OpenMetadataConnection,
                          service_name: str,
                          logger_level: workflow.LogLevels = workflow.LogLevels.DEBUG,
                          **source_config
                          ):
    run_profiler(openmetadataServerConnection,
                 ServiceType.Database,
                 service_name,
                 logger_level=logger_level,
                 **source_config
                 )


def run_profiler_messaging(openmetadataServerConnection: openMetadataConnection.OpenMetadataConnection,
                           service_name: str,
                           logger_level: workflow.LogLevels = workflow.LogLevels.DEBUG,
                           **source_config
                           ):
    run_profiler(openmetadataServerConnection,
                 ServiceType.Messaging,
                 service_name,
                 logger_level=logger_level,
                 **source_config
                 )


def run_profiler(openmetadataServerConnection: openMetadataConnection.OpenMetadataConnection,
                 service_type: ServiceType,
                 service_name: str,
                 logger_level: workflow.LogLevels = workflow.LogLevels.DEBUG,
                 **source_config
                 ):
    run_by_custom(openmetadataServerConnection=openmetadataServerConnection,
                  service_type=service_type,
                  service_name=service_name,
                  pipeline_type="profiler",
                  logger_level=logger_level,
                  **source_config
                  )


def run_by_custom(openmetadataServerConnection: openMetadataConnection.OpenMetadataConnection,
                  service_type: ServiceType,
                  service_name: str,
                  pipeline_type: str = "metadata",
                  logger_level: workflow.LogLevels = workflow.LogLevels.DEBUG,
                  **source_config
                  ):
    metadata = OpenMetadata(openmetadataServerConnection)
    service = get_service_by_metadata(metadata, service_type, service_name)

    ingestion_pipeline_config = {
        "name": "{}_{}".format(service_name, uuid.uuid1()),
        "pipelineType": pipeline_type,
        "loggerLevel": logger_level,
        "airflowConfig": {},
        "openMetadataServerConnection": openmetadataServerConnection,
        "service": {
            "id": service.id,
            "fullyQualifiedName": service.fullyQualifiedName.__root__,
            "name": service.name.__root__,
            #"type": SERVICE_TYPE_CLASS_MAP.get(service_type).__name__, #Bug:首字母要小写
            "type": "{}Service".format(service_type.value.lower()),
            "deleted": service.deleted,
        },
        "sourceConfig": workflow.SourceConfig(**{
            "config": json.loads(
                source_config.get("source_config_str")) if "source_config_str" in source_config else source_config,
        }),
    }

    ingestion_pipeline = IngestionPipeline(**ingestion_pipeline_config)
    _run(ingestion_pipeline)


def run_by_task(openmetadataServerConnection: openMetadataConnection.OpenMetadataConnection,
                task_fqn: str
                ):
    metadata = OpenMetadata(openmetadataServerConnection)

    ingestion_pipeline = metadata.get_by_name(
        entity=IngestionPipeline,
        fqn=task_fqn,
        fields="*",
        nullable=False
    )
    ingestion_pipeline.openMetadataServerConnection = openmetadataServerConnection
    _run(ingestion_pipeline)


def run_by_config(config: str):
    workflow_config_dict = json.loads(config)
    if "airflowConfig" not in workflow_config_dict:
        workflow_config_dict["airflowConfig"] = {}
    ingestion_pipeline = IngestionPipeline(**workflow_config_dict)
    _run(ingestion_pipeline)


def _run(ingestion_pipeline: IngestionPipeline):
    # we need to instantiate the secret manager in case secrets are passed
    SecretsManagerFactory(
        ingestion_pipeline.openMetadataServerConnection.secretsManagerProvider,
        ingestion_pipeline.openMetadataServerConnection.secretsManagerLoader,
    )

    # 获取工作流的类型
    workflow_type = ingestion_pipeline.pipelineType.value

    if workflow_type == PipelineType.metadata.value:
        workflow_config = build_metadata_workflow_config(ingestion_pipeline)
        metadata_ingestion_workflow(workflow_config)
    elif workflow_type == PipelineType.profiler.value:
        workflow_config = build_profiler_workflow_config(ingestion_pipeline)
        profiler_workflow(workflow_config)
    elif workflow_type == PipelineType.TestSuite.value:
        workflow_config = build_test_suite_workflow_config(ingestion_pipeline)
        test_suite_workflow(workflow_config)
    elif workflow_type == PipelineType.usage.value:
        workflow_config = build_usage_workflow_config(ingestion_pipeline)
        usage_workflow(workflow_config)
    elif workflow_type == PipelineType.lineage.value:
        workflow_config = build_lineage_workflow_config(ingestion_pipeline)
        metadata_ingestion_workflow(workflow_config)
    else:
        raise Exception("尚不知当前任务类型【%s】的处理" % workflow_type)
