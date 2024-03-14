package org.openmetadata.service.util;

public final class SQLUtils {

  private SQLUtils() {}

  public static String eq(String field, Object value) {
    return expression(field, "=", value);
  }

  public static String gt(String field, Object value) {
    return expression(field, ">", value);
  }

  public static String ge(String field, Object value) {
    return expression(field, ">=", value);
  }

  public static String lt(String field, Object value) {
    return expression(field, "<", value);
  }

  public static String le(String field, Object value) {
    return expression(field, "<=", value);
  }

  public static String ne(String field, Object value) {
    return expression(field, "!=", value);
  }

  public static String expression(String field, String operator, Object value) {
    if (value == null) {
      throw new NullPointerException("表达式的值不支持为 NULL");
    }

    String sqlValue;
    if (value instanceof Number) {
      sqlValue = String.valueOf(value);
    } else {
      sqlValue = String.format("'%s'", value);
    }

    return String.format("%s %s %s", field, operator, sqlValue);
  }
}
