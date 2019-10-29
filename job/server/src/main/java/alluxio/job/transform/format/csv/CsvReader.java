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
import alluxio.job.transform.format.Format;
import alluxio.job.transform.format.ReadWriterUtils;
import alluxio.job.transform.format.TableReader;
import alluxio.job.transform.format.TableRow;
import alluxio.job.transform.format.TableSchema;

import com.google.common.io.Closer;
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
  private final Closer mCloser;
  private final CsvSchema mSchema;

  /**
   * @param inputPath the input path
   * @throws IOException when failed to open the input
   */
  private CsvReader(Path inputPath) throws IOException {
    mCloser = Closer.create();
    try {
      Configuration conf = ReadWriterUtils.readNoCacheConf();
      mFs = mCloser.register(inputPath.getFileSystem(conf));
      CSVProperties props = new CSVProperties.Builder()
          .hasHeader()
          .build();
      Schema schema;
      try (InputStream inputStream = open(mFs, inputPath)) {
        String schemaName = inputPath.getName();
        if (schemaName.contains(".")) {
          schemaName = schemaName.substring(0, schemaName.indexOf("."));
        }
        schema = AvroCSV.inferSchema(schemaName, inputStream, props);
        mSchema = new CsvSchema(schema);
      }
      mReader = mCloser.register(
          new AvroCSVReader<>(open(mFs, inputPath), props, schema, Record.class, false));
    } catch (IOException e) {
      try {
        mCloser.close();
      } catch (IOException ioe) {
        e.addSuppressed(ioe);
      }
      throw e;
    }
  }

  private static InputStream open(FileSystem fs, Path path) throws IOException {
    InputStream stream = fs.open(path);
    if (Format.isGzipped(path.toString())) {
      stream = new GZIPInputStream(stream);
    }
    return stream;
  }

  /**
   * Creates a CSV reader.
   *
   * @param uri the URI to the input
   * @return the reader
   * @throws IOException when failed to create the reader
   */
  public static CsvReader create(AlluxioURI uri) throws IOException {
    return new CsvReader(new Path(uri.getScheme(), uri.getAuthority().toString(), uri.getPath()));
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
    mCloser.close();
  }
}
