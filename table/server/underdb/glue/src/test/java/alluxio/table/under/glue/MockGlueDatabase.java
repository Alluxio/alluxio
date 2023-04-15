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

package alluxio.table.under.glue;

import alluxio.grpc.table.FieldSchema;
import alluxio.grpc.table.Schema;
import alluxio.grpc.table.layout.hive.HiveBucketProperty;
import alluxio.grpc.table.layout.hive.Storage;
import alluxio.grpc.table.layout.hive.StorageFormat;

import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.SerDeInfo;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.List;

public class MockGlueDatabase {

  private MockGlueDatabase() {}

  private static final String DB_NAME = "testdb";
  private static final String TABLE_NAME = "testtb";
  private static final String COLUMN_NAME = "testColumn";
  private static final String INPUT_FORMAT = "testInputFormat";
  private static final String OUTPUT_FORMAT = "testOutputFormat";
  private static final String SERDE_LIB = "testSerdeLib";
  private static final String SD_LOCATION = "/testTable";
  private static final Integer NUM_OF_BUCKET = 1;

  public static Database glueTestDatabase() {
    return new Database()
        .withName(DB_NAME)
        .withDescription("test database")
        .withLocationUri("/testdb")
        .withParameters(Collections.emptyMap());
  }

  public static Table glueTestTable() {
    return new Table()
        .withDatabaseName(DB_NAME)
        .withName(TABLE_NAME)
        .withPartitionKeys(glueTestColumn(COLUMN_NAME))
        .withParameters(Collections.emptyMap())
        .withStorageDescriptor(glueTestStorageDescriptor());
  }

  public static Partition glueTestPartition(List<String> values) {
    return new Partition()
        .withDatabaseName(DB_NAME)
        .withTableName(TABLE_NAME)
        .withValues(values)
        .withParameters(ImmutableMap.of())
        .withStorageDescriptor(glueTestStorageDescriptor());
  }

  public static Column glueTestColumn(String columnName) {
    return new Column()
        .withName(columnName)
        .withType("string")
        .withComment("test column");
  }

  public static StorageDescriptor glueTestStorageDescriptor() {
    return new StorageDescriptor()
        .withBucketColumns(ImmutableList.of("testBucket"))
        .withColumns(ImmutableList.of(glueTestColumn(COLUMN_NAME)))
        .withParameters(ImmutableMap.of())
        .withSerdeInfo(new SerDeInfo()
            .withSerializationLibrary(SERDE_LIB)
            .withParameters(ImmutableMap.of()))
        .withInputFormat(INPUT_FORMAT)
        .withOutputFormat(OUTPUT_FORMAT)
        .withLocation(SD_LOCATION)
        .withNumberOfBuckets(NUM_OF_BUCKET);
  }

  public static Schema alluxioSchema() {
    Schema.Builder schemaBuilder = Schema.newBuilder();
    schemaBuilder.addAllCols(alluxioFieldSchema());
    return schemaBuilder.build();
  }

  public static List<FieldSchema> alluxioFieldSchema() {
    return ImmutableList.of(
        FieldSchema.newBuilder()
        .setName(COLUMN_NAME)
        .setType("string")
        .setComment("test column")
        .build()
    );
  }

  public static Storage alluxioStorage() {
    return Storage
        .newBuilder()
        .setStorageFormat(alluxioStorageFormat())
        .setLocation(SD_LOCATION)
        .setBucketProperty(HiveBucketProperty
            .newBuilder()
            .setBucketCount(NUM_OF_BUCKET)
            .build())
        .putAllSerdeParameters(ImmutableMap.of())
        .build();
  }

  public static StorageFormat alluxioStorageFormat() {
    return StorageFormat
        .newBuilder()
        .setInputFormat(INPUT_FORMAT)
        .setOutputFormat(OUTPUT_FORMAT)
        .putAllSerdelibParameters(ImmutableMap.of())
        .setSerde(SERDE_LIB)
        .build();
  }
}
