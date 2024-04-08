package com.xgit.openmetadata.client;

import com.xgit.openmetadata.client.config.ClientConfig;
import org.openmetadata.schema.services.connections.metadata.AuthProvider;

public abstract class OpenMetadataTest extends OpenMetadataBaseTest {

  private ClientConfig clientConfig = null;

  protected abstract ClientConfig initClientConfig();

  public ClientConfig getClientConfig() {
    if (null == clientConfig) {
      clientConfig = initClientConfig();
    }
    return clientConfig;
  }

  @Override
  protected String hostPort() {
    return getClientConfig().getHostPort();
  }

  @Override
  protected AuthProvider authProvider() {
    return getClientConfig().getAuthProvider();
  }

  @Override
  protected Object securityConfig() {
    return getClientConfig().getSecurityConfig();
  }
}
