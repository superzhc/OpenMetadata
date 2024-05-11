package org.openmetadata.service.secrets.converter;

import java.util.List;
import org.openmetadata.schema.services.connections.network.HttpConnection;
import org.openmetadata.schema.services.connections.network.auth.http.BasicAuth;
import org.openmetadata.schema.services.connections.network.auth.http.BearerToken;
import org.openmetadata.service.util.JsonUtils;

public class HttpConnectionClassConverter extends ClassConverter {
  private static final List<Class<?>> CONFIG_SOURCE_CLASSES = List.of(BasicAuth.class, BearerToken.class);

  public HttpConnectionClassConverter() {
    super(HttpConnection.class);
  }

  @Override
  public Object convert(Object object) {
    HttpConnection httpConnection = (HttpConnection) JsonUtils.convertValue(object, this.clazz);

    if (httpConnection.getAuthorization() != null) {
      tryToConvert(httpConnection.getAuthorization().getConfig(), CONFIG_SOURCE_CLASSES)
          .ifPresent(data -> httpConnection.getAuthorization().setConfig(data));
    }

    return httpConnection;
  }
}
