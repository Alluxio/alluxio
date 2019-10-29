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

import alluxio.AlluxioURI;
import alluxio.job.transform.Format;
import alluxio.job.transform.PartitionInfo;
import alluxio.job.transform.format.ReadWriterUtils;
import alluxio.job.transform.format.TableReader;
import alluxio.job.transform.format.TableRow;
import alluxio.job.transform.format.TableSchema;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.cli.csv.AvroCSV;
import org.apache.parquet.cli.csv.AvroCSVReader;
import org.apache.parquet.cli.csv.CSVProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 * A reader for reading {@link CsvRow}.
 */
public final class CsvReader implements TableReader {
  private static final Logger LOG = LoggerFactory.getLogger(CsvReader.class);

  private final FileSystem mFs;
  private final AvroCSVReader<Record> mReader;
  private final CsvSchema mSchema;

  /**
   * @param fs the hdfs compatible client
   * @param reader the CSV reader
   * @param schema the schema
   */
  private CsvReader(FileSystem fs, AvroCSVReader<Record> reader, Schema schema) {
    mFs = fs;
    mReader = reader;
    mSchema = new CsvSchema(schema);
  }

  private static InputStream open(FileSystem fs, Path path, boolean isGzipped) throws IOException {
    InputStream stream = fs.open(path);
    if (isGzipped) {
      stream = new GZIPInputStream(stream);
    }
    return stream;
  }

  /**
   * Creates a CSV reader.
   *
   * @param uri the URI to the input
   * @param pInfo the partition info
   * @return the reader
   * @throws IOException when failed to create the reader
   */
  public static CsvReader create(AlluxioURI uri, PartitionInfo pInfo) throws IOException {
    boolean isGzipped = pInfo.getFormat().equals(Format.GZIP_CSV);
    CSVProperties props = new CSVProperties.Builder()
        .hasHeader()
        .build();
    Path inputPath = new Path(uri.getScheme(), uri.getAuthority().toString(), uri.getPath());
    Configuration conf = ReadWriterUtils.readNoCacheConf();
    FileSystem fs = inputPath.getFileSystem(conf);
    Schema schema;
    try (InputStream inputStream = open(fs, inputPath, isGzipped)) {
      String schemaName = inputPath.getName();
      if (schemaName.contains(".")) {
        schemaName = schemaName.substring(0, schemaName.indexOf("."));
      }
      schema = AvroCSV.inferSchema(schemaName, inputStream, props);
    }
    AvroCSVReader<Record> reader = new AvroCSVReader<>(open(fs, inputPath, isGzipped), props,
        schema, Record.class, false);
    return new CsvReader(fs, reader, schema);
  }

  @Override
  public TableSchema getSchema() throws IOException {
    return mSchema;
  }

  @Override
  public TableRow read() throws IOException {
    return mReader.hasNext() ? new CsvRow(mReader.next()) : null;
  }

  @Override
  public void close() throws IOException {
    mReader.close();
    mFs.close();
  }
}
