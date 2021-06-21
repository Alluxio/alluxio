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

package alluxio.table.under.gdc;

import alluxio.grpc.table.FieldSchema;
import alluxio.grpc.table.Schema;

import alluxio.grpc.table.layout.hive.Storage;
import alluxio.grpc.table.layout.hive.StorageFormat;
import alluxio.table.common.udb.PathTranslator;

import com.google.cloud.bigquery.ExternalTableDefinition;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * GDC Utils.
 * */
public class GDCUtils {
  /* prevent instantiation */
  private GDCUtils() {}

  private static final Logger LOG = LoggerFactory.getLogger(GDCUtils.class);

  // conversion from https://prestodb.io/docs/current/connector/bigquery.html#data-types
  // mappings are lowercase because https://stackoverflow.com/a/55505613
  private static Map<String, String> sTypeConverter = new HashMap<String, String>() { {
        put("BOOLEAN", "bool");
        put("BYTES", "varbinary");
        put("DATE", "date");
        put("DATETIME", "timestamp");
        put("FLOAT", "double");
        put("GEOGRAPHY", "varchar");
        put("INTEGER", "bigint");
        put("NUMERIC", "decimal(38, 9)");
        put("RECORD", "row");
        put("STRING", "varchar");
        put("TIME", "time_with_time_zone");
        put("TIMESTAMP", "timestamp_with_time_zone");
    }
  };

  /**
   * Convert GDC field schema to alluxio proto.
   *
   * @param schema for the Google Data Catalog table
   * @return alluxio proto of schema
   */
  public static Schema toProtoSchema(com.google.cloud.bigquery.Schema schema) {
    Schema.Builder schemaBuilder = Schema.newBuilder();
    schemaBuilder.addAllCols(toProto(schema));
    return schemaBuilder.build();
  }

  /**
   * @param gdcSchema the hive schema
   * @return the proto representation
   */
  public static List<alluxio.grpc.table.FieldSchema> toProto(
      com.google.cloud.bigquery.Schema gdcSchema) {
    List<FieldSchema> list = new ArrayList<>();
    for (Field field : gdcSchema.getFields()) {
      String type = sTypeConverter.get(field.getType().toString());
      FieldSchema.Builder builder = FieldSchema.newBuilder()
          .setName(field.getName())
          .setType(type);
      if (field.getDescription() != null && !field.getDescription().isEmpty()) {
        builder.setComment(field.getDescription());
      }
      list.add(builder.build());
    }
    return list;
  }

  /**
   * Convert the GDC Table Definition and Translator information to Storage.
   *
   * @param table the gdc storage descriptor
   * @param translator the glue translator
   * @return storage proto
   * @throws IOException
   */
  public static Storage toProto(Table table, PathTranslator translator)
      throws IOException {
    if (table == null) {
      return Storage.getDefaultInstance();
    }
    String format = ((ExternalTableDefinition) table.getDefinition())
        .getFormatOptions().getType();

    Map<String, String> serdeLibMap = new HashMap<>();
    StorageFormat.Builder formatBuilder = StorageFormat.newBuilder();

    if (format.equals("PARQUET")) {
      serdeLibMap.put("serialization.format", "1");
      formatBuilder.setInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
          .setOutputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat")
          .setSerde("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
          .putAllSerdelibParameters(serdeLibMap);
    } else {
      formatBuilder.setInputFormat(format)
          .setOutputFormat(format)
          .setSerde(format);
    }
    String location = ((ExternalTableDefinition) table.getDefinition()).getSourceUris().get(0);

    return Storage.newBuilder().setStorageFormat(formatBuilder.build())
        .setLocation(translator.toAlluxioPath(location))
        .build();
  }
}
