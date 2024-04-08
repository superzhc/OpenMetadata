package com.xgit.openmetadata.client.config;

import org.openmetadata.schema.security.client.OpenMetadataJWTClientConfig;
import org.openmetadata.schema.services.connections.metadata.AuthProvider;

/**
 * 本地系统配置
 */
public class LocalServerConfig extends ClientConfig {

  public static final String HOST_PORT = "http://127.0.0.1:8585/api";
  public static final AuthProvider AUTH_PROVIDER = AuthProvider.OPENMETADATA;

  public static final OpenMetadataJWTClientConfig OPEN_METADATA_JWT_CLIENT_CONFIG;

  static {
    OPEN_METADATA_JWT_CLIENT_CONFIG = new OpenMetadataJWTClientConfig();
    OPEN_METADATA_JWT_CLIENT_CONFIG.setJwtToken(
        "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6Imhhbnl1bi1tZXRhZGF0YSIsImVtYWlsIjoiaGFueXVuLW1ldGFkYXRhQG9wZW5tZXRhZGF0YS5vcmciLCJpc0JvdCI6dHJ1ZSwidG9rZW5UeXBlIjoiQk9UIiwiaWF0IjoxNzExMzUwODg3LCJleHAiOm51bGx9.oFAogLZyjZS4tIhrHK2H30pTjv7h-RCKDJkSFX8IYiEDp8W6qM0rHhecp9gdR0HPF6dA4kW950yX0SlhW8obWF_OIavX9xXMCCfNA7AnImi8tEKjXVMIIr_FP8euCwG029-WjBpIjbDpzhHbF2IaP_BfzKi87yolD9vj02GDOjNNxW01qsgKMKz4JhL6l1w-_HPnaegWlnytBWMclL99xkif2BECOchhKWYPg1B2zctAtNxHXy7bz-f2XcLCTPmWpAsUDy5Sp1MFRjXjUfx2FY0G3x-WUpBMp5vs5b_qwSnKmCYwFh4FikwpU3ged43lc0m6Czb8__NFQIdB1F8lsA");
  }

  public LocalServerConfig() {
    super(HOST_PORT, AUTH_PROVIDER, OPEN_METADATA_JWT_CLIENT_CONFIG);
  }
}
