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

package alluxio.job.transform.format.parquet;

import alluxio.client.WriteType;
import alluxio.conf.PropertyKey;
import alluxio.job.transform.format.TableRow;
import alluxio.job.transform.format.TableSchema;
import alluxio.job.transform.format.TableWriter;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A writer for writing {@link ParquetRow}.
 */
public final class ParquetWriter implements TableWriter {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetWriter.class);
  // https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/src/main/java
  // /org/apache/parquet/hadoop/InternalParquetRecordWriter.java#L46
  private static final int MAX_IN_MEMORY_RECORDS = 10000;
  private static final int ROW_GROUP_SIZE =
      org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
  private static final String ALLUXIO_HADOOP_FILESYSTEM_DISABLE_CACHE =
      "fs.alluxio.impl.disable.cache";

  private final org.apache.parquet.hadoop.ParquetWriter<Record> mWriter;
  private long mRecordSize; // bytes
  private int mRows;

  private ParquetWriter(org.apache.parquet.hadoop.ParquetWriter<Record> writer) {
    mWriter = writer;
  }

  /**
   * Creates a Parquet writer.
   *
   * @param schema the schema
   * @param scheme the scheme of the output, e.g. "alluxio", "file"
   * @param output the output Parquet file
   * @return the writer
   * @throws IOException when failed to create the writer
   */
  public static ParquetWriter create(TableSchema schema, String scheme, String output)
      throws IOException {
    Configuration conf = new Configuration();
    conf.setEnum(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT.getName(), WriteType.THROUGH);
    // The cached filesystem might not be configured with the above write type.
    conf.setBoolean(ALLUXIO_HADOOP_FILESYSTEM_DISABLE_CACHE, true);
    ParquetSchema parquetSchema = schema.toParquet();
    return new ParquetWriter(AvroParquetWriter.<Record>builder(
        HadoopOutputFile.fromPath(new Path(scheme, "", output), conf))
        .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
        .withConf(conf)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withRowGroupSize(ROW_GROUP_SIZE)
        .withDictionaryPageSize(org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE)
        .withDictionaryEncoding(true)
        .withPageSize(org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE)
        .withDataModel(GenericData.get())
        .withSchema(parquetSchema.getSchema())
        .build());
  }

  @Override
  public void write(TableRow row) throws IOException {
    ParquetRow parquetRow = row.toParquet();
    mWriter.write(parquetRow.getRecord());
    mRows++;
    if (mRows == 1) {
      mRecordSize = mWriter.getDataSize();
    }
  }

  @Override
  public void close() throws IOException {
    mWriter.close();
  }

  @Override
  public int getRows() {
    return mRows;
  }

  @Override
  public long getBytes() {
    // getDataSize returns the on-disk size + in-memory size,
    // on-disk size takes compression and encoding into consideration,
    // but in-memory size does not.
    // This method returns the estimated lower bound of the on-disk size by subtracting an
    // estimated upper bound of in-memory size.
    // After closing, the on-disk size will be larger due to flushing the in-memory records to disk.
    return Math.max(0,
        mWriter.getDataSize() - Math.max(ROW_GROUP_SIZE, MAX_IN_MEMORY_RECORDS * mRecordSize));
  }
}
