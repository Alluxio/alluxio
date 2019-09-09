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

package alluxio.table.under.hive;

import alluxio.grpc.FieldTypeId;
import alluxio.grpc.Schema;
import alluxio.grpc.Type;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.util.ArrayList;
import java.util.List;

/**
 * Utilities for hive types.
 */
public class HiveUtils {
  private HiveUtils() {} // prevent instantiation

  /**
   * @param hiveSchema the hive schema
   * @return the proto representation
   */
  public static List<alluxio.grpc.FieldSchema> toProto(List<FieldSchema> hiveSchema) {
    List<alluxio.grpc.FieldSchema> list = new ArrayList<>();
    for (FieldSchema field : hiveSchema) {
      alluxio.grpc.FieldSchema aFieldSchema = alluxio.grpc.FieldSchema.newBuilder()
          .setName(field.getName())
          .setType(Type.newBuilder().setType(toProto(field.getType())))
          .build();
      list.add(aFieldSchema);
    }
    return list;
  }

  /**
   * @param hiveSchema the hive schema
   * @return the proto representation
   */
  public static Schema toProtoSchema(List<FieldSchema> hiveSchema) {
    Schema.Builder schemaBuilder = Schema.newBuilder();
    schemaBuilder.addAllCols(toProto(hiveSchema));
    return schemaBuilder.build();
  }

  private static FieldTypeId toProto(String hiveType) {
    switch (hiveType) {
      case "boolean": return FieldTypeId.BOOLEAN;
      case "tinyint": return FieldTypeId.INTEGER;
      case "smallint": return FieldTypeId.INTEGER;
      case "int": return FieldTypeId.INTEGER;
      case "integer": return FieldTypeId.INTEGER;
      case "bigint": return FieldTypeId.LONG;
      case "float": return FieldTypeId.FLOAT;
      case "double": return FieldTypeId.DOUBLE;
      case "decimal": return FieldTypeId.DECIMAL;
      case "numeric": return FieldTypeId.DECIMAL;
      case "date": return FieldTypeId.DATE;
      case "timestamp": return FieldTypeId.TIMESTAMP;
      case "string": return FieldTypeId.STRING;
      case "char": return FieldTypeId.STRING;
      case "varchar": return FieldTypeId.STRING;
      case "binary": return FieldTypeId.BINARY;
      default: // fall through
    }
    if (hiveType.startsWith("map<")) {
      return FieldTypeId.MAP;
    } else if (hiveType.startsWith("struct<")) {
      return FieldTypeId.STRUCT;
    } else if (hiveType.startsWith("decimal(")) {
      return FieldTypeId.DECIMAL;
    }
    throw new IllegalArgumentException("Unsupported hive type: " + hiveType);
  }
}
