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
import alluxio.job.transform.SchemaField;
import alluxio.job.transform.format.ReadWriterUtils;
import alluxio.job.transform.format.TableReader;
import alluxio.job.transform.format.TableRow;
import alluxio.job.transform.format.TableSchema;

import com.google.common.io.Closer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.cli.csv.AvroCSVReader;
import org.apache.parquet.cli.csv.CSVProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

/**
 * A reader for reading {@link CsvRow}.
 */
public final class CsvReader implements TableReader {
  private static final Logger LOG = LoggerFactory.getLogger(CsvReader.class);
  private static final String SKIP_HEADER = "skip.header.line.count";

  private final FileSystem mFs;
  private final AvroCSVReader<Record> mReader;
  private final Closer mCloser;
  private final CsvSchema mSchema;

  /**
   * @param inputPath the input path
   * @param pInfo the partition info
   * @throws IOException when failed to open the input
   */
  private CsvReader(Path inputPath, PartitionInfo pInfo) throws IOException {
    mCloser = Closer.create();
    try {
      Schema schema = buildSchema(pInfo.getFields());
      mSchema = new CsvSchema(schema);

      Configuration conf = ReadWriterUtils.readNoCacheConf();
      mFs = mCloser.register(inputPath.getFileSystem(conf));
      boolean isGzipped = pInfo.getFormat().equals(Format.GZIP_CSV);
      InputStream input = open(mFs, inputPath, isGzipped);

      CSVProperties props = buildProperties(pInfo.getProperties());

      mReader = mCloser.register(new AvroCSVReader<>(input, props, schema, Record.class, false));
    } catch (IOException e) {
      try {
        mCloser.close();
      } catch (IOException ioe) {
        e.addSuppressed(ioe);
      }
      throw e;
    }
  }

  private CSVProperties buildProperties(HashMap<String, String> properties) {
    CSVProperties.Builder propsBuilder = new CSVProperties.Builder();
    if (properties.containsKey(SKIP_HEADER) && Integer.parseInt(properties.get(SKIP_HEADER)) >= 1) {
      propsBuilder.hasHeader();
    }
    return propsBuilder.build();
  }

  private Schema buildSchema(ArrayList<SchemaField> fields) {
    // TODO(cc)
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
    return new CsvReader(new Path(uri.getScheme(), uri.getAuthority().toString(), uri.getPath()),
        pInfo);
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
