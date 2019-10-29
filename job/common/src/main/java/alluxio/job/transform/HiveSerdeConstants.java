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

package alluxio.job.transform;

public class HiveSerdeConstants {
  private HiveSerdeConstants() {} // Prevents initialization

  /**
   * Skip header.
   */
  public static final String LINES_TO_SKIP = "skip.header.line.count";
  public static final String FIELD_DELIM = "field.delim";
  public static final String SERIALIZATION_FORMAT = "serialization.format";
  public static final String PARQUET_SERDE_CLASS =
      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
  public static final String CSV_SERDE_CLASS = "org.apache.hadoop.hive.serde2.OpenCSVSerde";
  public static final String TEXT_FILE_INPUT_CLASS = "org.apache.hadoop.mapred.TextInputFormat";


  /**
   * Primitive types retrieved from
   * https://github.com/apache/hive/blob/master/serde/src/gen/thrift/gen-javabean/org/apache/hadoop/
   * hive/serde/serdeConstants.java
   */
  public static final class PrimitiveTypes {
    private PrimitiveTypes() {} // Prevents initialization

    // TODO(cc): what's void?
    private static final String VOID = "void";
    private static final String BOOLEAN = "boolean";
    private static final String TINYINT = "tinyint";
    private static final String SMALLINT = "smallint";
    private static final String INT = "int";
    private static final String BIGINT = "bigint";
    private static final String FLOAT = "float";
    private static final String DOUBLE = "double";
    private static final String STRING = "string";
    private static final String VARCHAR = "varchar";
    private static final String CHAR = "char";
    private static final String DATE = "date";
    private static final String DATETIME = "datetime";
    private static final String TIMESTAMP = "timestamp";
    private static final String INTERVAL_YEAR_MONTH = "interval_year_month";
    private static final String INTERVAL_DAY_TIME = "interval_day_time";
    private static final String DECIMAL = "decimal";
    private static final String BINARY = "binary";
    private static final String TIMESTAMP_LOCAL = "timestamp with local time zone";

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
