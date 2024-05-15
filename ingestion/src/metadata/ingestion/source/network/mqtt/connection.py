from typing import Optional, Any
import paho.mqtt.client as mqtt
from metadata.generated.schema.entity.automations.workflow import (
    Workflow as AutomationWorkflow,
)
from metadata.generated.schema.entity.services.connections.network.mqttConnection import (
    MQTTConnection
)
from metadata.ingestion.connections.test_connections import test_connection_steps
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


def get_connection(
        connection: MQTTConnection
) -> mqtt.Client:
    def on_connect(client, userdata, flags, rc, properties):
        if rc != 0:
            raise Exception(f"Failed to connect, return code {rc}")

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    if connection.user:
        client.username = connection.user
    if connection.password:
        client.password = connection.password.get_secret_value()

    client.on_connect = on_connect

    return client


def test_connection(
        metadata: OpenMetadata,
        client: mqtt.Client,
        service_connection: MQTTConnection,
        automation_workflow: Optional[AutomationWorkflow] = None,
) -> None:
    """
    Test connection. This can be executed either as part
    of a metadata workflow or during an Automation Workflow
    """

    def custom_executor():
        client.connect(service_connection.host, service_connection.port)
        if service_connection.topic:
            client.subscribe(service_connection.topic)
        else:
            # 若配置订阅的主题，走一次连接即可
            client.loop_start()
            client.loop_stop()

    test_fn = {"CheckAccess": custom_executor}

    test_connection_steps(
        metadata=metadata,
        test_fn=test_fn,
        service_type=service_connection.type.value,
        automation_workflow=automation_workflow,
    )
