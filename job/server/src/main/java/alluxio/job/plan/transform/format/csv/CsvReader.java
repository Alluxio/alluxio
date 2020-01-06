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

import alluxio.AlluxioURI;
import alluxio.job.plan.transform.Format;
import alluxio.job.plan.transform.HiveConstants;
import alluxio.job.plan.transform.PartitionInfo;
import alluxio.job.plan.transform.format.JobPath;
import alluxio.job.plan.transform.format.ReadWriterUtils;
import alluxio.job.plan.transform.format.TableReader;
import alluxio.job.plan.transform.format.TableRow;
import alluxio.job.plan.transform.format.TableSchema;

import com.google.common.io.Closer;
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
import java.util.Map;
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
   * @param pInfo the partition info
   * @throws IOException when failed to open the input
   */
  private CsvReader(JobPath inputPath, PartitionInfo pInfo) throws IOException {
    mCloser = Closer.create();
    try {
      mSchema = new CsvSchema(pInfo.getFields());

      Configuration conf = ReadWriterUtils.readNoCacheConf();
      mFs = inputPath.getFileSystem(conf);
      boolean isGzipped = pInfo.getFormat(inputPath.getName()).equals(Format.GZIP_CSV);
      InputStream input = open(mFs, inputPath, isGzipped);

      CSVProperties props = buildProperties(pInfo.getTableProperties(),
          pInfo.getSerdeProperties());

      try {
        mReader = mCloser.register(new AvroCSVReader<>(input, props, mSchema.getReadSchema(),
            Record.class, false));
      } catch (RuntimeException e) {
        throw new IOException("Failed to create CSV reader", e);
      }
    } catch (IOException e) {
      try {
        mCloser.close();
      } catch (IOException ioe) {
        e.addSuppressed(ioe);
      }
      throw e;
    }
  }

  private CSVProperties buildProperties(Map<String, String> tableProperties,
      Map<String, String> serdeProperties) {
    CSVProperties.Builder propsBuilder = new CSVProperties.Builder();
    if (tableProperties.containsKey(HiveConstants.LINES_TO_SKIP)) {
      propsBuilder.linesToSkip(Integer.parseInt(tableProperties.get(HiveConstants.LINES_TO_SKIP)));
    }
    if (serdeProperties.containsKey(HiveConstants.FIELD_DELIM)) {
      propsBuilder.delimiter(serdeProperties.get(HiveConstants.FIELD_DELIM));
    }
    return propsBuilder.build();
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
    JobPath path = new JobPath(uri.getScheme(), uri.getAuthority().toString(), uri.getPath());
    return new CsvReader(path, pInfo);
  }

  @Override
  public TableSchema getSchema() throws IOException {
    return mSchema;
  }

  @Override
  public TableRow read() throws IOException {
    try {
      return mReader.hasNext() ? new CsvRow(mSchema, mReader.next()) : null;
    } catch (Throwable e) {
      throw new IOException(e.getMessage(), e.getCause());
    }
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }
}
