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

package alluxio.job.plan.transform.format.tables;

import alluxio.AlluxioURI;
import alluxio.job.plan.transform.format.TableRow;
import alluxio.job.plan.transform.format.TableSchema;
import alluxio.job.plan.transform.format.TableWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Writes a stream of rows to a list of tables, when {@link Committer} determines that the current
 * table should be committed/completed, the table file is completed and a new table file is created.
 */
public class TablesWriter implements TableWriter {
  private static final Logger LOG = LoggerFactory.getLogger(TablesWriter.class);
  private static final String FILE_NAME_PATTERN = "part-%d.parquet";

  private final Committer mCommitter;
  private final TableSchema mSchema;
  private final AlluxioURI mOutputDir;
  private TableWriter mWriter;
  private int mPart;
  private int mRows;
  private int mBytes;

  private TablesWriter(TableSchema schema, Committer committer, AlluxioURI outputDir,
      TableWriter initialWriter) {
    mOutputDir = outputDir;
    mSchema = schema;
    mWriter = initialWriter;
    mCommitter = committer;
    mPart = 0;
    mRows = 0;
    mBytes = 0;
  }

  /**
   * @param schema the table schema
   * @param committer the committer
   * @param outputDir the output directory
   * @return a new writer
   * @throws IOException when failed to create an internal table writer
   */
  public static TablesWriter create(TableSchema schema, Committer committer, AlluxioURI outputDir)
      throws IOException {
    return new TablesWriter(schema, committer, outputDir, createWriter(schema, outputDir, 0));
  }

  @Override
  public void write(TableRow row) throws IOException {
    mWriter.write(row);
    if (mCommitter.shouldCommit(mWriter)) {
      mRows += mWriter.getRows();
      mBytes += mWriter.getBytes();
      mWriter.close();
      mWriter = createWriter(mSchema, mOutputDir, ++mPart);
    }
  }

  @Override
  public void close() throws IOException {
    mWriter.close();
  }

  @Override
  public int getRows() {
    return mRows + mWriter.getRows();
  }

  @Override
  public long getBytes() {
    return mBytes + mWriter.getBytes();
  }

  private static TableWriter createWriter(TableSchema schema, AlluxioURI outputDir, int part)
      throws IOException {
    String filename = String.format(FILE_NAME_PATTERN, part);
    return TableWriter.create(schema, outputDir.join(filename));
  }
}
