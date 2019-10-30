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

package alluxio.job.transform.format.csv;

import alluxio.job.transform.HiveConstants;
import alluxio.job.transform.SchemaField;
import alluxio.job.transform.format.TableSchema;
import alluxio.job.transform.format.parquet.ParquetSchema;

import com.google.common.base.Objects;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import javax.validation.constraints.NotNull;

/**
 * CSV table schema in Avro format.
 */
public final class CsvSchema implements TableSchema {
  private static final String JAVA_CLASS_FLAG = "java-class";

  /** The schema from Alluxio table master. */
  private final ArrayList<SchemaField> mAlluxioSchema;
  /** The schema for reading from CSV. */
  private final Schema mReadSchema;
  /** The schema for writing. */
  private final Schema mWriteSchema;

  /**
   * {@link CsvReader} uses {@link org.apache.parquet.cli.csv.AvroCSVReader} to read records from
   * CSV. {@link org.apache.parquet.cli.csv.AvroCSVReader} internally uses
   * {@link org.apache.parquet.cli.csv.RecordBuilder}, which does not support BYTES, and some
   * logical types such as DECIMAL. So we use readSchema to let AvroCSVReader be able to read the
   * record, then when writing out the record, we interpret the record according to
   * writeSchema.
   *
   * For example, if a column is of type DECIMAL, in readSchema, the type will be STRING,
   * but in writeSchema, it's logical type DECIMAL backed by BYTES.
   *
   * @param schema the schema from Alluxio table master
   * @throws IOException when failed to initialize schema
   */
  public CsvSchema(@NotNull ArrayList<SchemaField> schema) throws IOException{
    mAlluxioSchema = schema;
    mReadSchema = buildReadSchema(Schema.Type.RECORD.getName(), schema);
    mWriteSchema = buildWriteSchema(Schema.Type.RECORD.getName(), schema);
  }

  /**
   * @return the schema from Alluxio table master
   */
  public ArrayList<SchemaField> getAlluxioSchema() {
    return mAlluxioSchema;
  }

  /**
   * @return the schema for reading from CSV
   */
  public Schema getReadSchema() {
    return mReadSchema;
  }

  /**
   * @return the schema for writing
   */
  public Schema getWriteSchema() {
    return mWriteSchema;
  }

  @Override
  public ParquetSchema toParquet() {
    return new ParquetSchema(mWriteSchema);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mAlluxioSchema, mReadSchema, mWriteSchema);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CsvSchema)) {
      return false;
    }
    CsvSchema that = (CsvSchema) o;
    return Objects.equal(mAlluxioSchema, that.mAlluxioSchema)
        && Objects.equal(mReadSchema, that.mReadSchema)
        && Objects.equal(mWriteSchema, that.mWriteSchema);
  }

  private Schema buildReadSchema(String name, ArrayList<SchemaField> fields) throws IOException {
    SchemaBuilder.FieldAssembler<Schema> assembler =
        SchemaBuilder.record(name).fields();
    for (SchemaField field : fields) {
      assembler = buildReadField(assembler, field);
    }
    return assembler.endRecord();
  }

  private Schema buildWriteSchema(String name, ArrayList<SchemaField> fields) throws IOException {
    SchemaBuilder.FieldAssembler<Schema> assembler =
        SchemaBuilder.record(name).fields();
    for (SchemaField field : fields) {
      assembler = buildWriteField(assembler, field);
    }
    return assembler.endRecord();
  }

  private Schema makeOptional(Schema schema, boolean optional) {
    return optional ? Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), schema))
        : schema;
  }

  private SchemaBuilder.FieldAssembler<Schema> buildSharedField(
      SchemaBuilder.FieldAssembler<Schema> assembler, SchemaField field) throws IOException {
    // Builds the fields that are the same for read and write schema.
    String name = field.getName();
    String type = field.getType();
    boolean optional = field.isOptional();
    if (type.equals(HiveConstants.PrimitiveTypes.BOOLEAN)) {
      return optional ? assembler.optionalBoolean(name) : assembler.requiredBoolean(name);
    } else if (type.equals(HiveConstants.PrimitiveTypes.TINYINT)
        || type.equals(HiveConstants.PrimitiveTypes.SMALLINT)
        || type.equals(HiveConstants.PrimitiveTypes.INT)
    ) {
      return optional ? assembler.optionalInt(name) : assembler.requiredInt(name);
    } else if (type.equals(HiveConstants.PrimitiveTypes.DOUBLE)) {
      return optional ? assembler.optionalDouble(name) : assembler.requiredDouble(name);
    } else if (type.equals(HiveConstants.PrimitiveTypes.FLOAT)) {
      return optional ? assembler.optionalFloat(name) : assembler.requiredFloat(name);
    } else if (type.equals(HiveConstants.PrimitiveTypes.BIGINT)) {
      return optional ? assembler.optionalLong(name) : assembler.requiredLong(name);
    } else if (type.equals(HiveConstants.PrimitiveTypes.STRING)) {
      return optional ? assembler.optionalString(name) : assembler.requiredString(name);
    } else if (type.startsWith(HiveConstants.PrimitiveTypes.CHAR)) {
      Schema schema = SchemaBuilder.builder().stringBuilder().prop(JAVA_CLASS_FLAG,
          Character.class.getCanonicalName())
          .endString();
      schema = makeOptional(schema, optional);
      return assembler.name(name).type(schema).noDefault();
    } else if (type.startsWith(HiveConstants.PrimitiveTypes.VARCHAR)) {
      return optional ? assembler.optionalString(name) : assembler.requiredString(name);
    }
    throw new IOException("Unsupported type " + type + " for field " + name);
  }

  private SchemaBuilder.FieldAssembler<Schema> buildReadField(
      SchemaBuilder.FieldAssembler<Schema> assembler, SchemaField field) throws IOException {
    if (!CsvUtils.isReadWriteTypeInconsistent(field.getType())) {
      return buildSharedField(assembler, field);
    }

    String name = field.getName();
    boolean optional = field.isOptional();
    // 1. Use string for arbitrary precision for decimal.
    // 2. Use string for UTF-8 encoded binary values.
    // 3. Use string for CSV text format of date and timestamp:
    //    date: 2019-01-02
    //    timestamp: 2019-10-29 10:17:42.338
    return optional ? assembler.optionalString(name) : assembler.requiredString(name);
  }

  private SchemaBuilder.FieldAssembler<Schema> buildWriteField(
      SchemaBuilder.FieldAssembler<Schema> assembler, SchemaField field) throws IOException {
    if (!CsvUtils.isReadWriteTypeInconsistent(field.getType())) {
      return buildSharedField(assembler, field);
    }

    String name = field.getName();
    String type = field.getType();
    boolean optional = field.isOptional();
    if (type.startsWith(HiveConstants.PrimitiveTypes.DECIMAL)) {
      String param = type.substring(8, type.length() - 1).trim();
      String[] params = param.split(",");
      int precision = Integer.parseInt(params[0].trim());
      int scale = Integer.parseInt(params[1].trim());
      Schema schema = LogicalTypes.decimal(precision, scale)
          .addToSchema(Schema.create(Schema.Type.BYTES));
      schema = makeOptional(schema, optional);
      return assembler.name(name).type(schema).noDefault();
    }
    if (type.equals(HiveConstants.PrimitiveTypes.BINARY)) {
      return optional ? assembler.optionalBytes(name) : assembler.requiredBytes(name);
    }
    if (type.equals(HiveConstants.PrimitiveTypes.DATE)) {
      Schema schema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
      schema = makeOptional(schema, optional);
      return assembler.name(name).type(schema).noDefault();
    } else if (type.equals(HiveConstants.PrimitiveTypes.TIMESTAMP)) {
      Schema schema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
      schema = makeOptional(schema, optional);
      return assembler.name(name).type(schema).noDefault();
    }
    throw new IOException("Unsupported type " + type + " for field " + name);
  }
}
