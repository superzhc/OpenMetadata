package com.xgit.openmetadata.client.config;

import lombok.Getter;
import org.openmetadata.schema.services.connections.metadata.AuthProvider;

@Getter
public class ClientConfig {
  private String hostPort;
  private AuthProvider authProvider;
  private Object securityConfig;

  public ClientConfig(String hostPort, AuthProvider authProvider, Object securityConfig) {
    this.hostPort = hostPort;
    this.authProvider = authProvider;
    this.securityConfig = securityConfig;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String hostPort;
    private AuthProvider authProvider;
    private Object securityConfig;

    public Builder hostPort(String hostPort) {
      this.hostPort = hostPort;
      return this;
    }

    public Builder authProvider(AuthProvider authProvider) {
      this.authProvider = authProvider;
      return this;
    }

    public Builder securityConfig(Object securityConfig) {
      this.securityConfig = securityConfig;
      return this;
    }

    public ClientConfig build() {
      return new ClientConfig(this.hostPort, this.authProvider, this.securityConfig);
    }
  }
}
