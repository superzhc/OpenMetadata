import unittest
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
    OpenMetadataJWTClientConfig,
)
from metadata.ingestion.ometa.ometa_api import OpenMetadata

HOST_PORT="http://localhost:8585/api"
AUTH_PROVIDER="openmetadata"
TOKEN="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6InNka191c2FnZSIsImVtYWlsIjoic2RrX3VzYWdlQGV4YW1wbGUuY29tIiwiaXNCb3QiOnRydWUsInRva2VuVHlwZSI6IkJPVCIsImlhdCI6MTcwMTA2NjA4NCwiZXhwIjpudWxsfQ.QHMNZHvuh6Sdayr3XcfIIBetY-NJ2srZWmwNibqa4fvqoAxoqwyWBBInQ8kF2jgRNZo8b6L0_dJRCHTfvO8xBvs-BsOg6V5URAFjCW-UMLrVXA3MJs19mVE1L_qb0t-zNB8PfvQ9XS6WLVF2ozK32XKXwkHssz_MpOCbBbo6qUEj-vtt56scXh-e9cILBApCOd023pXJX3HmWhtG_tVV24oHy9wU1GeU0m61OsDMXiVsyBWN0oM3Lh3mwdvFd9uT1bz6-9hJHAjKBG-LQKWT0yjhIXuWqin-_-Sjlc0IMTmJ-mwkxT1_hl8m77Cv3b2Pi5XGe3gcwyvSoPhSakq5WQ"

class OpenMetadataTest(unittest.TestCase):
    server_config = OpenMetadataConnection(
        hostPort=HOST_PORT,
        authProvider=AUTH_PROVIDER,
        securityConfig=OpenMetadataJWTClientConfig(jwtToken=TOKEN),
    )
    metadata = OpenMetadata(server_config)

    assert metadata.health_check()

    def test_init(self):
        b=self.metadata.health_check()
        print("是否连通：",b)

if __name__=="__main__":
    suites=unittest.TestSuite()
    suites.addTest(OpenMetadata("test_init"))

    runner=unittest.TextTestRunner()
    runner.run(suites)