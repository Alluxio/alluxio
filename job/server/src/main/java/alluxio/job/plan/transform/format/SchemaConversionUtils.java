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

package alluxio.job.plan.transform.format;

import alluxio.job.plan.transform.FieldSchema;
import alluxio.job.plan.transform.HiveConstants;
import alluxio.job.plan.transform.format.csv.CsvUtils;
import alluxio.job.plan.transform.format.csv.Decimal;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Utility Class for converting schema to Parquet.
 */
public class SchemaConversionUtils {
  private static final String JAVA_CLASS_FLAG = "java-class";

  /**
   * Builds write schema.
   * @param fields the fields
   * @return the write schema
   */
  public static Schema buildWriteSchema(List<FieldSchema> fields)
      throws IOException {
    SchemaBuilder.FieldAssembler<Schema> assembler =
        SchemaBuilder.record(Schema.Type.RECORD.getName()).fields();
    for (FieldSchema field : fields) {
      assembler = buildWriteField(assembler, field);
    }
    return assembler.endRecord();
  }

  private static Schema makeOptional(Schema schema) {
    return Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), schema));
  }

  /**
   * Builds the fields that are consistent with {@link Schema}.
   * @param assembler the field assembler
   * @param field field to add to the field assembler
   * @return new field assembler with the existing fields + the new field
   */
  public static SchemaBuilder.FieldAssembler<Schema> buildConsistentField(
      SchemaBuilder.FieldAssembler<Schema> assembler, FieldSchema field) throws IOException {
    String name = field.getName();
    String type = field.getType();

    switch (HiveConstants.Types.getHiveConstantType(type)) {
      case HiveConstants.Types.BOOLEAN:
        return assembler.optionalBoolean(name);
      case HiveConstants.Types.TINYINT:
      case HiveConstants.Types.SMALLINT:
      case HiveConstants.Types.INT:
        return assembler.optionalInt(name);
      case HiveConstants.Types.DOUBLE:
        return assembler.optionalDouble(name);
      case HiveConstants.Types.FLOAT:
        return assembler.optionalFloat(name);
      case HiveConstants.Types.BIGINT:
        return assembler.requiredLong(name);
      case HiveConstants.Types.STRING:
      case HiveConstants.Types.VARCHAR:
        return assembler.optionalString(name);
      case HiveConstants.Types.CHAR:
        Schema schema = SchemaBuilder.builder().stringBuilder().prop(JAVA_CLASS_FLAG,
            Character.class.getCanonicalName())
            .endString();
        schema = makeOptional(schema);
        return assembler.name(name).type(schema).noDefault();
      default:
        throw new IOException("Unsupported type " + type + " for field " + name);
    }
  }

  private static SchemaBuilder.FieldAssembler<Schema> buildWriteField(
      SchemaBuilder.FieldAssembler<Schema> assembler, FieldSchema field) throws IOException {
    if (!CsvUtils.isReadWriteTypeInconsistent(field.getType())) {
      return buildConsistentField(assembler, field);
    }

    String name = field.getName();
    String type = field.getType();
    Schema schema;

    switch (HiveConstants.Types.getHiveConstantType(type)) {
      case HiveConstants.Types.DECIMAL:
        Decimal decimal = new Decimal(type);
        schema = LogicalTypes.decimal(decimal.getPrecision(), decimal.getScale())
            .addToSchema(Schema.create(Schema.Type.BYTES));
        schema = makeOptional(schema);
        return assembler.name(name).type(schema).noDefault();
      case HiveConstants.Types.BINARY:
        return assembler.optionalBytes(name);
      case HiveConstants.Types.DATE:
        schema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
        schema = makeOptional(schema);
        return assembler.name(name).type(schema).noDefault();
      case HiveConstants.Types.TIMESTAMP:
        schema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        schema = makeOptional(schema);
        return assembler.name(name).type(schema).noDefault();
      default:
        throw new IOException("Unsupported type " + type + " for field " + name);
    }
  }
}
