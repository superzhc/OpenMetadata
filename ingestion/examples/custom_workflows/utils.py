import time
import json
import uuid
from typing import Union

from metadata.generated.schema.metadataIngestion.workflow import LogLevels
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.generated.schema.entity.type import (
    entityReference
)
from metadata.generated.schema.entity.services.serviceType import ServiceType
from metadata.generated.schema.entity.services.connections.metadata import (
    openMetadataConnection
)
from metadata.generated.schema.entity.services.ingestionPipelines.ingestionPipeline import (
    IngestionPipeline,
    PipelineType,
    AirflowConfig,
)
from metadata.generated.schema.entity.services.dashboardService import DashboardService
from metadata.generated.schema.entity.services.databaseService import DatabaseService
from metadata.generated.schema.entity.services.messagingService import MessagingService
from metadata.generated.schema.entity.services.metadataService import MetadataService
from metadata.generated.schema.entity.services.mlmodelService import MlModelService
from metadata.generated.schema.entity.services.pipelineService import PipelineService
from metadata.generated.schema.entity.services.searchService import SearchService
from metadata.generated.schema.entity.services.storageService import StorageService
from metadata.generated.schema.entity.services.networkService import NetworkService
from metadata.generated.schema.metadataIngestion import (
    applicationPipeline,
    dashboardServiceMetadataPipeline,
    databaseServiceMetadataPipeline,
    databaseServiceProfilerPipeline,
    databaseServiceQueryLineagePipeline,
    databaseServiceQueryUsagePipeline,
    dataInsightPipeline,
    dbtPipeline,
    messagingServiceMetadataPipeline,
    metadataToElasticSearchPipeline,
    mlmodelServiceMetadataPipeline,
    pipelineServiceMetadataPipeline,
    searchServiceMetadataPipeline,
    storageServiceMetadataPipeline,
    testSuitePipeline,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    SourceConfig,
    LogLevels,
)
from metadata.generated.schema.metadataIngestion.testSuitePipeline import (
    TestSuiteConfigType,
    TestSuitePipeline,
)
from metadata.generated.schema.type.entityReference import EntityReference
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
from metadata.utils.secrets.secrets_manager_factory import SecretsManagerFactory

SERVICE_TYPE_CLASS_MAP = {
    ServiceType.Database: DatabaseService,
    ServiceType.Messaging: MessagingService,
    ServiceType.Metadata: MetadataService,
    ServiceType.Network: NetworkService,
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


def run_by_custom(openmetadataServerConnection: openMetadataConnection.OpenMetadataConnection,
                  service_name: str,
                  service_type: str = "Database",
                  pipeline_type: str = "metadata",
                  **source_config
                  ):
    """
    自定义提取任务进行执行
    :param service_name:
    :param service_type: 支持 Database，Messaging
    :param pipeline_type: 支持 metadata，profiler
    :param source_config:
    :return:
    """

    metadata = OpenMetadata(openmetadataServerConnection)

    serviceType: ServiceType = ServiceType(service_type)
    service = get_service_by_metadata(metadata, serviceType, service_name)

    ingestion_pipeline_name = f"{service_name}_{pipeline_type}"
    ingestion_pipeline_fullname = f"{service_name}.{ingestion_pipeline_name}"

    sourceConfig = SourceConfig()
    if PipelineType(pipeline_type) is PipelineType.metadata:
        if serviceType is ServiceType.Database:
            sourceConfig.config = databaseServiceMetadataPipeline.DatabaseServiceMetadataPipeline(**source_config)
        elif serviceType is ServiceType.Messaging:
            sourceConfig.config = messagingServiceMetadataPipeline.MessagingServiceMetadataPipeline(**source_config)
        else:
            raise Exception(f"尚不支持服务类型[{serviceType.value()}]的[{pipeline_type}]类型任务处理")
    elif PipelineType(pipeline_type) is PipelineType.profiler:
        sourceConfig.config = databaseServiceProfilerPipeline.DatabaseServiceProfilerPipeline(**source_config)
    else:
        raise Exception(f"尚不支持当前任务类型[{pipeline_type}]的处理")

    ingestion_pipeline = IngestionPipeline(
        name=ingestion_pipeline_name,
        fullyQualifiedName=ingestion_pipeline_fullname,
        loggerLevel=LogLevels.DEBUG,
        pipelineType=PipelineType(pipeline_type),
        openMetadataServerConnection=openmetadataServerConnection,
        airflowConfig=AirflowConfig(),
        service=EntityReference(
            id=service.id,
            type="{}Service".format(serviceType.value.lower()),
            name=service.name.__root__,
            fullyQualifiedName=service.fullyQualifiedName.__root__,
        ),
        sourceConfig=sourceConfig,
    )

    _run(ingestion_pipeline, False)


def run_testSuite(openmetadataServerConnection: openMetadataConnection.OpenMetadataConnection,
                  service_name: str,
                  database: str,
                  schema: str,
                  table: str,
                  ):
    table_fullname = f"{service_name}.{database}.{schema}.{table}"
    ingestion_pipeline_service_type = "testSuite"
    ingestion_pipeline_service_name = f"{table_fullname}.{ingestion_pipeline_service_type}"
    ingestion_pipeline_name = f"{table}_{''.join(ingestion_pipeline_service_type[:1].upper() + ingestion_pipeline_service_type[1:])}"
    ingestion_pipeline_fullname = f"{ingestion_pipeline_service_name}.{ingestion_pipeline_name}"

    ingestion_pipeline = IngestionPipeline(
        name=ingestion_pipeline_name,
        fullyQualifiedName=ingestion_pipeline_fullname,
        loggerLevel=LogLevels.DEBUG,
        pipelineType=PipelineType.TestSuite,
        sourceConfig=SourceConfig(
            config=TestSuitePipeline(
                type=TestSuiteConfigType.TestSuite,
                entityFullyQualifiedName=table_fullname
            )
        ),
        openMetadataServerConnection=openmetadataServerConnection,
        airflowConfig=AirflowConfig(),
        service=EntityReference(
            id=uuid.uuid1(),
            type=ingestion_pipeline_service_type,
            name=ingestion_pipeline_service_name,
        ),
    )

    _run(ingestion_pipeline, False)


def run_testSuite_task(openmetadataServerConnection: openMetadataConnection.OpenMetadataConnection,
                       service_name: str,
                       database: str,
                       schema: str,
                       table: str,
                       ):
    task_fqn = f"{service_name}.{database}.{schema}.{table}.testSuite.{table}_TestSuite"
    run_by_task(openmetadataServerConnection, task_fqn)


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
    _run(ingestion_pipeline, True)


def run_by_config(config: str):
    workflow_config_dict = json.loads(config)
    if "airflowConfig" not in workflow_config_dict:
        workflow_config_dict["airflowConfig"] = {}
    ingestion_pipeline = IngestionPipeline(**workflow_config_dict)
    _run(ingestion_pipeline)


def _run(ingestion_pipeline: IngestionPipeline, realIngestionPipeline: bool = True):
    # we need to instantiate the secret manager in case secrets are passed
    SecretsManagerFactory(
        ingestion_pipeline.openMetadataServerConnection.secretsManagerProvider,
        ingestion_pipeline.openMetadataServerConnection.secretsManagerLoader,
    )

    # 获取工作流的类型
    workflow_type = ingestion_pipeline.pipelineType.value

    if workflow_type == PipelineType.metadata.value:
        workflow_config = build_metadata_workflow_config(ingestion_pipeline)
        if not realIngestionPipeline:
            workflow_config.ingestionPipelineFQN = None
        metadata_ingestion_workflow(workflow_config)
    elif workflow_type == PipelineType.profiler.value:
        workflow_config = build_profiler_workflow_config(ingestion_pipeline)
        if not realIngestionPipeline:
            workflow_config.ingestionPipelineFQN = None
        profiler_workflow(workflow_config)
    elif workflow_type == PipelineType.TestSuite.value:
        workflow_config = build_test_suite_workflow_config(ingestion_pipeline)
        if not realIngestionPipeline:
            workflow_config.ingestionPipelineFQN = None
        # 因界面无法配置日志级别，此处手动控制
        workflow_config.workflowConfig.loggerLevel = LogLevels.DEBUG
        test_suite_workflow(workflow_config)
    elif workflow_type == PipelineType.usage.value:
        workflow_config = build_usage_workflow_config(ingestion_pipeline)
        if not realIngestionPipeline:
            workflow_config.ingestionPipelineFQN = None
        usage_workflow(workflow_config)
    elif workflow_type == PipelineType.lineage.value:
        workflow_config = build_lineage_workflow_config(ingestion_pipeline)
        if not realIngestionPipeline:
            workflow_config.ingestionPipelineFQN = None
        metadata_ingestion_workflow(workflow_config)
    else:
        raise Exception("尚不知当前任务类型【%s】的处理" % workflow_type)
