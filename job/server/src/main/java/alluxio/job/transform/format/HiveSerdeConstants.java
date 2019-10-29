/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.job.transform.format;

public class HiveSerdeConstants {
  private HiveSerdeConstants() {} // Prevents initialization

  /**
   * Skip header.
   */
  public static final String SKIP_HEADER = "skip.header.line.count";
  public static final String FIELD_DELIM = "field.delim";

  /**
   * Primitive types retrieved from
   * https://github.com/apache/hive/blob/master/serde/src/gen/thrift/gen-javabean/org/apache/hadoop/
   * hive/serde/serdeConstants.java
   */
  public static final class PrimitiveTypes {
    private PrimitiveTypes() {} // Prevents initialization

    // TODO(cc): what's void?
    public static final String VOID = "void";
    public static final String BOOLEAN = "boolean";
    public static final String TINYINT = "tinyint";
    public static final String SMALLINT = "smallint";
    public static final String INT = "int";
    public static final String BIGINT = "bigint";
    public static final String FLOAT = "float";
    public static final String DOUBLE = "double";
    public static final String STRING = "string";
    public static final String VARCHAR = "varchar";
    public static final String CHAR = "char";
    public static final String DATE = "date";
    public static final String DATETIME = "datetime";
    public static final String TIMESTAMP = "timestamp";
    public static final String INTERVAL_YEAR_MONTH = "interval_year_month";
    public static final String INTERVAL_DAY_TIME = "interval_day_time";
    public static final String DECIMAL = "decimal";
    public static final String BINARY = "binary";
    public static final String TIMESTAMP_LOCAL = "timestamp with local time zone";

    public static boolean isBoolean(String type) {
      return type.equals(BOOLEAN);
    }

    public static boolean isInt(String type) {
      return type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INT);
    }

    public static boolean isLong(String type) {
      return type.equals(BIGINT);
    }

    public static boolean isFloat(String type) {
      return type.equals(FLOAT);
    }

    public static boolean isDouble(String type) {
      return type.equals(DOUBLE) || type.equals(DECIMAL);
    }

    public static boolean isString(String type) {
      return type.equals(STRING) || type.equals(DATE) || type.equals(DATETIME)
          || type.equals(INTERVAL_YEAR_MONTH) || type.equals(INTERVAL_DAY_TIME)
          || type.equals(TIMESTAMP) || type.equals(TIMESTAMP_LOCAL)
          || type.equals(CHAR) || type.equals(VARCHAR);
    }

    public static boolean isBytes(String type) {
      return type.equals(BINARY);
    }
  }
}
