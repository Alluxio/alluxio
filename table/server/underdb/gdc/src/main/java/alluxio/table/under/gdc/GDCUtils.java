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

import com.google.cloud.bigquery.Field;

import java.util.ArrayList;
import java.util.List;

/**
 * GDC Utils.
 * */
public class GDCUtils {
  /* prevent instantiation */
  private GDCUtils() {}

  /**
   * Convert GDC field schema to alluxio proto.
   *
   * @param schema for the Google Data Catalog table
   * @return alluxio proto of schema
   */
  public static Schema toProtoSchema(com.google.cloud.bigquery.Schema schema) {
    List<FieldSchema> list = new ArrayList<>();
    for (Field field : schema.getFields()) {
      FieldSchema.Builder builder = FieldSchema.newBuilder()
          .setName(field.getName())
          .setType(field.getType().toString());
      if (field.getDescription() != null && !field.getDescription().isEmpty()) {
        builder.setComment(field.getDescription());
      }
      list.add(builder.build());
    }
    return Schema.newBuilder().addAllCols(list).build();
  }
}
