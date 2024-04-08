package com.xgit.openmetadata.client;


import org.apache.commons.lang.NotImplementedException;
import org.openmetadata.client.gateway.OpenMetadata;
import org.openmetadata.client.security.*;
import org.openmetadata.schema.security.client.*;
import org.openmetadata.schema.services.connections.metadata.AuthProvider;
import org.openmetadata.schema.services.connections.metadata.OpenMetadataConnection;

public abstract class OpenMetadataBaseTest {

  private OpenMetadata openMetadata = null;

  public OpenMetadataBaseTest() {
  }

  public OpenMetadata apiClient() {
    if (openMetadata == null) {
      openMetadata = new OpenMetadata(openMetadataConnection());
    }

    return openMetadata;
  }

  private OpenMetadataConnection openMetadataConnection() {
    OpenMetadataConnection connection = new OpenMetadataConnection();
    /*对于Client只需要用到如下两个配置*/
    setHostPort(connection);
    setAuth(connection);

    return connection;
  }

  private OpenMetadataConnection setHostPort(final OpenMetadataConnection openMetadataConnection) {
    openMetadataConnection.setHostPort(hostPort());
    return openMetadataConnection;
  }

  protected abstract String hostPort();

  private OpenMetadataConnection setAuth(final OpenMetadataConnection openMetadataConnection) {
    openMetadataConnection.setAuthProvider(authProvider());
    openMetadataConnection.setSecurityConfig(securityConfig());
    return openMetadataConnection;
  }

  abstract protected AuthProvider authProvider();

  protected Object securityConfig() {
    switch (authProvider()) {
      case NO_AUTH:
        return null;
      case GOOGLE:
        return googleSSOClientConfig();
      case OKTA:
        return oktaSSOClientConfig();
      case AUTH_0:
        return auth0SSOClientConfig();
      case CUSTOM_OIDC:
        return customOIDCSSOClientConfig();
      case AZURE:
        return azureSSOClientConfig();
      case OPENMETADATA:
        return openMetadataJWTClientConfig();
    }
    return null;
  }

  protected OpenMetadataJWTClientConfig openMetadataJWTClientConfig() {
    throw new NotImplementedException("尚未设置 JWT Token");
  }

  protected GoogleSSOClientConfig googleSSOClientConfig() {
    throw new NotImplementedException("尚未设置 Google SSO");
  }

  protected OktaSSOClientConfig oktaSSOClientConfig() {
    throw new NotImplementedException("尚未设置 Okta SSO");
  }

  protected Auth0SSOClientConfig auth0SSOClientConfig() {
    throw new NotImplementedException("尚未设置 Auth0 SSO");
  }

  protected CustomOIDCSSOClientConfig customOIDCSSOClientConfig() {
    throw new NotImplementedException("尚未设置 Custom OIDC SSO");
  }

  protected AzureSSOClientConfig azureSSOClientConfig() {
    throw new NotImplementedException("尚未设置 Azure SSO");
  }
}
