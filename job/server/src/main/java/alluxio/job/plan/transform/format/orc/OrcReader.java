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

package alluxio.job.plan.transform.format.orc;

import alluxio.AlluxioURI;
import alluxio.job.plan.transform.format.JobPath;
import alluxio.job.plan.transform.format.ReadWriterUtils;
import alluxio.job.plan.transform.format.TableReader;
import alluxio.job.plan.transform.format.TableRow;
import alluxio.job.plan.transform.format.TableSchema;

import com.google.common.io.Closer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

import java.io.IOException;
import java.util.List;

/**
 * The Orc reader.
 */
public final class OrcReader implements TableReader {

  private final Closer mCloser;
  private final OrcSchema mSchema;
  private final Reader mReader;
  private final RecordReader mRows;
  private final List<String> mFieldNames;

  /**
   * The current processing batch of the Orc reader for the read() operation.
   */
  private VectorizedRowBatch mCurrentBatch;
  /**
   * The row position inside the vectorized row batch to return next for read().
   */
  private int mCurrentBatchPosition;

  private OrcReader(JobPath inputPath) throws IOException {
    mCloser = Closer.create();
    try {

      Configuration conf = ReadWriterUtils.readNoCacheConf();

      mReader = mCloser.register(OrcFile.createReader(inputPath, OrcFile.readerOptions(conf)));
      mFieldNames = mReader.getSchema().getFieldNames();
      mRows = mReader.rows();

      mSchema = new OrcSchema(mReader);
    } catch (IOException e) {
      try {
        mCloser.close();
      } catch (IOException ioe) {
        e.addSuppressed(ioe);
      }
      throw e;
    }
  }

  /**
   * @param uri the alluxio uri of the orc file
   * @return new instance of OrcReader
   */
  public static OrcReader create(AlluxioURI uri) throws IOException {
    JobPath path = new JobPath(uri.getScheme(), uri.getAuthority().toString(), uri.getPath());
    return new OrcReader(path);
  }

  @Override
  public TableSchema getSchema() {
    return mSchema;
  }

  @Override
  public TableRow read() throws IOException {
    if (mCurrentBatch == null || mCurrentBatch.size <= mCurrentBatchPosition) {
      mCurrentBatch = mReader.getSchema().createRowBatch();
      mCurrentBatchPosition = 0;
      if (!mRows.nextBatch(mCurrentBatch)) {
        return null;
      }
    }

    return new OrcRow(mSchema, mCurrentBatch, mCurrentBatchPosition++, mFieldNames);
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }
}
