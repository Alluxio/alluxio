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

package alluxio.job.plan.transform;

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
   * ORC serde class.
   */
  public static final String ORC_SERDE_CLASS = "org.apache.hadoop.hive.ql.io.orc.OrcSerde";
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
   * Hive Types.
   */
  public static final class Types {
    private Types() {} // Prevents initialization

    // TODO(cc): how to support the following?
    // public static final String VOID = "void";
    // public static final String INTERVAL_YEAR_MONTH = "interval_year_month";
    // public static final String INTERVAL_DAY_TIME = "interval_day_time";
    // public static final String TIMESTAMP_LOCAL = "timestamp with local time zone";
    // public static final String DATETIME = "datetime";

    /** Hive bool type. */
    public static final String BOOLEAN = "boolean";
    /** Hive tiny int type. */
    public static final String TINYINT = "tinyint";
    /** Hive small int type. */
    public static final String SMALLINT = "smallint";
    /** Hive int type. */
    public static final String INT = "int";
    /** Hive big int type. */
    public static final String BIGINT = "bigint";
    /** Hive float type. */
    public static final String FLOAT = "float";
    /** Hive double type. */
    public static final String DOUBLE = "double";
    /** Hive string type. */
    public static final String STRING = "string";
    /** Hive varchar type. */
    public static final String VARCHAR = "varchar";
    /** Hive char type. */
    public static final String CHAR = "char";
    /** Hive date type. */
    public static final String DATE = "date";
    /** Hive timestamp type. */
    public static final String TIMESTAMP = "timestamp";
    /** Hive decimal type. */
    public static final String DECIMAL = "decimal";
    /** Hive binary type. */
    public static final String BINARY = "binary";

    /**
     * Filters out parts of type information to match the types constant for type checking.
     *
     * @param type the type
     * @return type name matching the types constants
     */
    public static String getHiveConstantType(String type) {
      // filters out the non hive type information from types like "char(10)"

      int i = type.indexOf('(');
      if (i == -1) {
        return type;
      }
      return type.substring(0, i);
    }
  }
}
