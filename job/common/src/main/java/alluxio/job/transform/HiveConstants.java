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

/**
 * Constants related to Hive.
 *
 * @see <a href="https://github.com/apache/hive/blob/master/serde/src/gen/thrift/gen-javabean/org/
 * apache/hadoop/hive/serde/serdeConstants.java">SerdeConstants.java</a>
 */
public class HiveConstants {
  private HiveConstants() {} // Prevents initialization

  /**
   * Number of lines to skip when reading from CSV.
   */
  public static final String LINES_TO_SKIP = "skip.header.line.count";
  /**
   * Field delimiter for CSV.
   */
  public static final String FIELD_DELIM = "field.delim";
  /**
   * Serialization format.
   */
  public static final String SERIALIZATION_FORMAT = "serialization.format";
  /**
   * Parquet serde class.
   */
  public static final String PARQUET_SERDE_CLASS =
      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
  /**
   * CSV serde class.
   */
  public static final String CSV_SERDE_CLASS = "org.apache.hadoop.hive.serde2.OpenCSVSerde";
  /**
   * Text input format.
   */
  public static final String TEXT_INPUT_FORMAT_CLASS = "org.apache.hadoop.mapred.TextInputFormat";
  /**
   * Parquet input format.
   */
  public static final String PARQUET_INPUT_FORMAT_CLASS =
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
  /**
   * Parquet output format.
   */
  public static final String PARQUET_OUTPUT_FORMAT_CLASS =
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";

  /**
   * Primitive types supported by Hive.
   */
  public static final class PrimitiveTypes {
    private PrimitiveTypes() {} // Prevents initialization

    // TODO(cc): how to support VOID?
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

    /**
     * @param type the type
     * @return whether is CSV boolean
     */
    public static boolean isBoolean(String type) {
      return type.equals(BOOLEAN);
    }

    /**
     * @param type the type
     * @return whether is CSV int
     */
    public static boolean isInt(String type) {
      return type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INT);
    }

    /**
     * @param type the type
     * @return whether is CSV long
     */
    public static boolean isLong(String type) {
      return type.equals(BIGINT);
    }

    /**
     * @param type the type
     * @return whether is CSV float
     */
    public static boolean isFloat(String type) {
      return type.equals(FLOAT);
    }

    /**
     * @param type the type
     * @return whether is CSV double
     */
    public static boolean isDouble(String type) {
      return type.equals(DOUBLE) || type.startsWith(DECIMAL);
    }

    /**
     * @param type the type
     * @return whether is CSV string
     */
    public static boolean isString(String type) {
      return type.equals(STRING) || type.equals(DATE) || type.equals(DATETIME)
          || type.equals(INTERVAL_YEAR_MONTH) || type.equals(INTERVAL_DAY_TIME)
          || type.equals(TIMESTAMP) || type.equals(TIMESTAMP_LOCAL)
          || type.startsWith(CHAR) || type.startsWith(VARCHAR)
          || type.equals(BINARY);
    }

    /**
     * @param type the type
     * @return whether is CSV bytes
     */
    public static boolean isBytes(String type) {
      return false;
    }
  }
}
