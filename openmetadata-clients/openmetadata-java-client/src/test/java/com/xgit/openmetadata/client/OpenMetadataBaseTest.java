package com.xgit.openmetadata.client;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.junit.Test;
import org.openmetadata.client.gateway.OpenMetadata;
import org.openmetadata.schema.security.client.OpenMetadataJWTClientConfig;
import org.openmetadata.schema.services.connections.metadata.AuthProvider;
import org.openmetadata.schema.services.connections.metadata.OpenMetadataConnection;

public class OpenMetadataBaseTest {
  private static final String JWT_TOKEN =
      "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6IkFsbEF1dGgiLCJlbWFpbCI6IkFsbEF1dGhAeGdpdC5jb20iLCJpc0JvdCI6dHJ1ZSwidG9rZW5UeXBlIjoiQk9UIiwiaWF0IjoxNzA4NDk2MTExLCJleHAiOm51bGx9.GRsZtM6i-ty1w71wg6HOxFmYQHDFXDH278x-G861jmaxuDLSCiIjRE0UiVlbNlsKQXoDbQIAT20eehfjgJ2Bp2HwUYBF8obunkTAkPv6WACJ741y5PQ-a59AWBYQlNhYviKxlUtneqDbQs88wN0iJL8FR_pdaMhYzLraSZs1FdA6mC_bLjfLyU6aSHrnmh7C6vEYBZS4NKDXmAp9iqp_upm_p0bmk7KUbY540TbA7ilcAAvYoZtxfYp86irtTOEq-yVni2J37XMLyVqNy3cVFT9ZcNS5sPDsTFB011fqKW-aaSFM45nXGt5A0eYEHaBrKLM_kUrdt-WRL2gjIXDpKg";
  private static final String HOST_PORT = "http://127.0.0.1:8585/api";

  private OpenMetadata openMetadata = null;

  private LocalDateTime current;

  public OpenMetadataBaseTest() {
    current = LocalDateTime.now();
  }

  protected OpenMetadataConnection openMetadataConnection() {
    OpenMetadataConnection connection = new OpenMetadataConnection();
    setHostPort(connection);
    setAuth(connection);

    return connection;
  }

  protected OpenMetadataConnection setHostPort(
      final OpenMetadataConnection openMetadataConnection) {
    openMetadataConnection.setHostPort(HOST_PORT);
    return openMetadataConnection;
  }

  protected OpenMetadataConnection setAuth(final OpenMetadataConnection openMetadataConnection) {
    OpenMetadataJWTClientConfig openMetadataJWTClientConfig = new OpenMetadataJWTClientConfig();
    openMetadataJWTClientConfig.setJwtToken(JWT_TOKEN);
    openMetadataConnection.setSecurityConfig(openMetadataJWTClientConfig);

    openMetadataConnection.setAuthProvider(AuthProvider.OPENMETADATA);

    return openMetadataConnection;
  }

  public OpenMetadata getClient() {
    if (openMetadata == null) {
      openMetadata = new OpenMetadata(openMetadataConnection());
    }

    return openMetadata;
  }

  // region 工具方法
  public String year() {
    return currentFormat("yyyy");
  }

  public String month() {
    return currentFormat("yyyyMM");
  }

  public String day() {
    return currentFormat("yyyyMMdd");
  }

  public String hour() {
    return currentFormat("yyyyMMddHH");
  }

  public String minute() {
    return currentFormat("yyyyMMddHHmm");
  }

  public String currentFormat(String format) {
    if (null == format || format.trim().length() == 0) {
      format = "yyyyMMddHHmmssSSS";
    }
    return current.format(DateTimeFormatter.ofPattern(format));
  }

  // endregion

  @Test
  public void testVersion() {
    System.out.println("OpenMetadata version: " + String.join(".", getClient().getClientVersion()));
  }
}
