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

package alluxio.job.plan.transform.format.parquet;

import alluxio.AlluxioURI;
import alluxio.job.plan.transform.PartitionInfo;
import alluxio.job.plan.transform.format.JobPath;
import alluxio.job.plan.transform.format.ReadWriterUtils;
import alluxio.job.plan.transform.format.TableRow;
import alluxio.job.plan.transform.format.TableSchema;
import alluxio.job.plan.transform.format.TableWriter;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
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
  private static final String DEFAULT_COMPRESSION_CODEC = CompressionCodecName.SNAPPY.name();

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
   * @param uri the URI to the output
   * @return the writer
   */
  public static ParquetWriter create(TableSchema schema, AlluxioURI uri)
      throws IOException {
    return ParquetWriter.create(schema, uri, ROW_GROUP_SIZE, true, DEFAULT_COMPRESSION_CODEC);
  }

  /**
   * Creates a parquet writer based on the partitionInfo.
   *
   * @param schema the schema
   * @param uri the URI to the output
   * @param partitionInfo the partitionInfo (default configuration is used if null)
   * @return the writer
   */
  public static ParquetWriter create(TableSchema schema, AlluxioURI uri,
                                     @Nullable PartitionInfo partitionInfo) throws IOException {
    String compressionCodec = DEFAULT_COMPRESSION_CODEC;
    if (partitionInfo != null) {
      compressionCodec = partitionInfo.getSerdeProperties().getOrDefault(
          PartitionInfo.PARQUET_COMPRESSION, DEFAULT_COMPRESSION_CODEC);
    }
    return ParquetWriter.create(schema, uri, ROW_GROUP_SIZE, true, compressionCodec);
  }

  /**
   * Creates a Parquet writer specifying a row group size and whether to have dictionary enabled.
   *
   * @param schema the schema
   * @param uri the URI to the output
   * @param rowGroupSize the row group size
   * @param enableDictionary whether to enable dictionary
   * @return the writer
   */
  public static ParquetWriter create(TableSchema schema, AlluxioURI uri, int rowGroupSize,
                                     boolean enableDictionary) throws IOException {
    return ParquetWriter.create(schema, uri, rowGroupSize, enableDictionary,
        DEFAULT_COMPRESSION_CODEC);
  }

  /**
   * Creates a Parquet writer specifying a row group size.
   *
   * @param schema the schema
   * @param uri the URI to the output
   * @param rowGroupSize the row group size
   * @param enableDictionary whether to enable dictionary
   * @param compressionCodec the compression codec name
   * @return the writer
   */
  public static ParquetWriter create(TableSchema schema, AlluxioURI uri, int rowGroupSize,
                                     boolean enableDictionary, String compressionCodec)
      throws IOException {
    Configuration conf = ReadWriterUtils.writeThroughConf();
    ParquetSchema parquetSchema = schema.toParquet();
    return new ParquetWriter(AvroParquetWriter.<Record>builder(
        HadoopOutputFile.fromPath(
            new JobPath(uri.getScheme(), uri.getAuthority().toString(), uri.getPath()), conf))
        .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
        .withConf(conf)
        .withCompressionCodec(CompressionCodecName.fromConf(compressionCodec))
        .withRowGroupSize(rowGroupSize)
        .withDictionaryPageSize(org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE)
        .withDictionaryEncoding(enableDictionary)
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
