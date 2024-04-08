package com.xgit.openmetadata.client.config;

import org.openmetadata.schema.security.client.OpenMetadataJWTClientConfig;
import org.openmetadata.schema.services.connections.metadata.AuthProvider;

/**
 * 演示系统配置
 */
public class DemoServerConfig extends ClientConfig {
  public static final String HOST_PORT = "http://127.0.0.1:8585/api";
  public static final AuthProvider AUTH_PROVIDER = AuthProvider.OPENMETADATA;

  public static final OpenMetadataJWTClientConfig OPEN_METADATA_JWT_CLIENT_CONFIG;

  static {
    OPEN_METADATA_JWT_CLIENT_CONFIG = new OpenMetadataJWTClientConfig();
    OPEN_METADATA_JWT_CLIENT_CONFIG.setJwtToken(
        ""
            );
  }

  public DemoServerConfig() {
    super(HOST_PORT, AUTH_PROVIDER, OPEN_METADATA_JWT_CLIENT_CONFIG);
  }
}
