import json
from typing import Optional, Any, cast
from requests import (
    request,
    Session,
)
from requests.auth import (
    AuthBase,
    HTTPBasicAuth,
)

from metadata.generated.schema.entity.automations.workflow import (
    Workflow as AutomationWorkflow,
)
from metadata.generated.schema.entity.services.connections.network.httpConnection import (
    HttpConnection,
    AuthType,
)
from metadata.generated.schema.entity.services.connections.network.http import (
    basicAuth,
    bearerToken,
)
from metadata.ingestion.connections.test_connections import test_connection_steps
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


class BearerTokenAuth(AuthBase):
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers["Authorization"] = f"Bearer {self.token}"
        return r


def get_connection(
        connection: HttpConnection
) -> Session:
    return Session()


def test_connection(
        metadata: OpenMetadata,
        client: Session,
        service_connection: HttpConnection,
        automation_workflow: Optional[AutomationWorkflow] = None,
) -> None:
    """
    Test connection. This can be executed either as part
    of a metadata workflow or during an Automation Workflow
    """

    url = f"{service_connection.protocol.value.lower()}://{service_connection.hostPort}{service_connection.path}"

    auth = None
    if service_connection.authorization:
        if AuthType.BasicAuth == service_connection.authorization.type:
            auth_config: basicAuth.BasicAuth = cast(basicAuth.BasicAuth, service_connection.authorization.config)
            auth = HTTPBasicAuth(auth_config.username, auth_config.password.get_secret_value())
        elif AuthType.BearerToken == service_connection.authorization.type:
            auth_config: bearerToken.BearerToken = cast(bearerToken.BearerToken, service_connection.authorization.config)
            auth = BearerTokenAuth(auth_config.token)

    def custom_executor():
        resp = client.request(service_connection.method.value.lower(), url,
                              headers=service_connection.headers,
                              params=service_connection.query,
                              data=json.loads(service_connection.body) if service_connection.body else None,
                              auth=auth,
                              )
        resp.raise_for_status()

    test_fn = {"CheckAccess": custom_executor}

    test_connection_steps(
        metadata=metadata,
        test_fn=test_fn,
        service_type=service_connection.type.value,
        automation_workflow=automation_workflow,
    )
