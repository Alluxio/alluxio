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

package alluxio.job.plan.transform.format.csv;

import alluxio.job.plan.transform.FieldSchema;
import alluxio.job.plan.transform.format.SchemaConversionUtils;
import alluxio.job.plan.transform.format.TableSchema;
import alluxio.job.plan.transform.format.parquet.ParquetSchema;

import com.google.common.base.Objects;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.io.IOException;
import java.util.ArrayList;

import javax.validation.constraints.NotNull;

/**
 * CSV table schema in Avro format.
 */
public final class CsvSchema implements TableSchema {

  /** The schema from Alluxio table master. */
  private final ArrayList<FieldSchema> mAlluxioSchema;
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
  public CsvSchema(@NotNull ArrayList<FieldSchema> schema) throws IOException {
    mAlluxioSchema = schema;
    mReadSchema = buildReadSchema(Schema.Type.RECORD.getName(), schema);
    mWriteSchema = SchemaConversionUtils.buildWriteSchema(schema);
  }

  /**
   * @return the schema from Alluxio table master
   */
  public ArrayList<FieldSchema> getAlluxioSchema() {
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

  private Schema buildReadSchema(String name, ArrayList<FieldSchema> fields) throws IOException {
    SchemaBuilder.FieldAssembler<Schema> assembler =
        SchemaBuilder.record(name).fields();
    for (FieldSchema field : fields) {
      assembler = buildReadField(assembler, field);
    }
    return assembler.endRecord();
  }

  private SchemaBuilder.FieldAssembler<Schema> buildReadField(
      SchemaBuilder.FieldAssembler<Schema> assembler, FieldSchema field) throws IOException {
    if (!CsvUtils.isReadWriteTypeInconsistent(field.getType())) {
      return SchemaConversionUtils.buildConsistentField(assembler, field);
    }

    String name = field.getName();
    // 1. Use string for arbitrary precision for decimal.
    // 2. Use string for UTF-8 encoded binary values.
    // 3. Use string for CSV text format of date and timestamp:
    //    date: 2019-01-02
    //    timestamp: 2019-10-29 10:17:42.338
    return assembler.optionalString(name);
  }
}
