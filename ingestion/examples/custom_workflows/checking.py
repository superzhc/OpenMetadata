"""
检测用
"""

import json

from metadata.generated.schema.entity.services.ingestionPipelines.ingestionPipeline import (
    IngestionPipeline,
    PipelineType,
)
from metadata.utils.secrets.secrets_manager_factory import SecretsManagerFactory
from openmetadata_managed_apis.workflows.ingestion.metadata import build_metadata_workflow_config
from openmetadata_managed_apis.workflows.ingestion.profiler import build_profiler_workflow_config


def check_workflow_config(workflow_config_str: str):
    workflow_config_dict = json.loads(workflow_config_str)
    airflow_pipeline = IngestionPipeline(**workflow_config_dict)

    # we need to instantiate the secret manager in case secrets are passed
    SecretsManagerFactory(
        airflow_pipeline.openMetadataServerConnection.secretsManagerProvider,
        airflow_pipeline.openMetadataServerConnection.secretsManagerLoader,
    )

    dag_type = airflow_pipeline.pipelineType.value
    if PipelineType.metadata.value == dag_type:
        build_metadata_workflow_config(airflow_pipeline)
    elif PipelineType.profiler.value == dag_type:
        build_profiler_workflow_config(airflow_pipeline)
    else:
        raise Exception(f"尚不支持任务类型为{dag_type}的配置检测")

    print("检测成功")


if __name__ == "__main__":
    workflow_config_str = '''
    {"id": "d2a33a13-d9fd-47df-954f-451359e0bb87", 
    "name": "mysql10_90_18_88_metadata_atiradeon", "displayName": "mysql10_90_18_88_metadata_atiradeon", "description": null, "pipelineType": "metadata", 
    "owner": {"id": "ddc68a8f-b2ee-4ca8-a40b-2072e92e73ce", "type": "user", "name": "admin", "fullyQualifiedName": "admin", "description": null, "displayName": null, "deleted": false, "href": null}, 
    "fullyQualifiedName": "\\\"mysql10.90.18.88\\\".mysql10_90_18_88_metadata_atiradeon", 
    "sourceConfig": {"config": {"type": "DatabaseMetadata", "markDeletedTables": true, "includeTables": true, "includeViews": true, "includeTags": true, "includeStoredProcedures": true, "queryLogDuration": 1, "queryParsingTimeoutLimit": 300, "useFqnForFiltering": false, "schemaFilterPattern": {"includes": [], "excludes": []}, "tableFilterPattern": {"includes": [".*atiradeon.*"], "excludes": []}, "databaseFilterPattern": {"includes": [], "excludes": []}}}, 
    "openMetadataServerConnection": {"clusterName": "openmetadata", "type": "OpenMetadata", "hostPort": "http://10.90.20.249:8585/api", "authProvider": "openmetadata", "verifySSL": "no-ssl", "sslConfig": null, "securityConfig": {"jwtToken": "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJlbWFpbCI6ImluZ2VzdGlvbi1ib3RAb3Blbm1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3MTA2NzYxNTgsImV4cCI6bnVsbH0.Xto5GPnfWFpfhGhj1tjSTonix-cCa2GXpkoxy-jwL9OfEA3ObLixKRgGKnJxsjpiijg_h6MPjEG6JufnEULX-Wj9HyznO2oJ-ORMqRSEKmWMiChIMw7Dg3OmzadZRSR3Gayogi-EzOg0XBLP-XNT947dzYZBtvncOSchxTheoYWekZBFq2TxjF8QhwEkiwlAusneoVgTr-Y1UlRqpLAJCdEn-WWOPqwQ5WxwsOTPHt4gcSoDzlMDq1W7rHCAyRpkNmU_Gb7vCnumtYgV4T0VTQO3sEDTLUBE2XFgB5vd1XYUgU3p4Ac2C4Zt3Oyvmd693im65zTkJ-Q7shAb79siug"}, "secretsManagerProvider": "noop", "secretsManagerLoader": "noop", "apiVersion": "v1", "includeTopics": true, "includeTables": true, "includeDashboards": true, "includePipelines": true, "includeMlModels": true, "includeUsers": true, "includeTeams": true, "includeGlossaryTerms": true, "includeTags": true, "includePolicy": true, "includeMessagingServices": true, "enableVersionValidation": true, "includeDatabaseServices": true, "includePipelineServices": true, "limitRecords": 1000, "forceEntityOverwriting": false, "storeServiceConnection": true, "elasticsSearch": null, "supportsDataInsightExtraction": true, "supportsElasticSearchReindexingExtraction": true, "extraHeaders": null}, 
    "airflowConfig": {"pausePipeline": false, "concurrency": 1, "startDate": "2024-05-06T00:00:00+00:00", "endDate": null, "pipelineTimezone": "UTC", "retries": 0, "retryDelay": 300, "pipelineCatchup": false, "scheduleInterval": "0 * * * *", "maxActiveRuns": 1, "workflowTimeout": null, "workflowDefaultView": "tree", "workflowDefaultViewOrientation": "LR", "email": null}, 
    "service": {"id": "557d4f5b-ccdd-4d3d-add7-f879a517eb12", "type": "databaseService", "name": "mysql10.90.18.88", "fullyQualifiedName": "\\\"mysql10.90.18.88\\\"", "description": "mysql8.0\u6d4b\u8bd5", "displayName": null, "deleted": false, "href": null}, "pipelineStatuses": null, 
    "loggerLevel": "DEBUG", 
    "deployed": false, "enabled": true, "href": "http://10.90.20.249:8585/api/v1/services/ingestionPipelines/d2a33a13-d9fd-47df-954f-451359e0bb87", "version": 0.1, "updatedAt": 1715075173045, "updatedBy": "admin", "changeDescription": null, "deleted": false, "provider": "user"}
    '''

    check_workflow_config(workflow_config_str)
