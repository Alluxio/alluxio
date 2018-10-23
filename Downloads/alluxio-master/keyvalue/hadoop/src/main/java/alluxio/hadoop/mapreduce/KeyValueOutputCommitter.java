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

package alluxio.hadoop.mapreduce;

import alluxio.annotation.PublicApi;
import alluxio.client.keyvalue.KeyValueSystem;
import alluxio.exception.AlluxioException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Extension of {@link FileOutputCommitter} where creating, completing, or deleting a {@link
 * KeyValueSystem} in different phases of a job's or task's lifecycle is considered.
 * <p>
 * This committer must be used along with {@link KeyValueOutputFormat} to merge the key-value stores
 * created by each Reducer into one key-value store under the MapReduce output directory.
 */
@PublicApi
@ThreadSafe
public final class KeyValueOutputCommitter extends FileOutputCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(KeyValueOutputCommitter.class);

  private static final KeyValueSystem KEY_VALUE_SYSTEM = KeyValueSystem.Factory.create();

  /**
   * Constructor.
   *
   * @param outputPath the job's output path, or null if the output committer is a noop
   * @param taskContext the task's context
   */
  public KeyValueOutputCommitter(Path outputPath, TaskAttemptContext taskContext)
      throws IOException {
    super(outputPath, taskContext);
  }

  /**
   * @param taskContext MapReduce task configuration
   * @return true if the task output directory exists, otherwise false
   */
  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
    Path taskOutputPath = new Path(KeyValueOutputFormat.getTaskOutputURI(taskContext).toString());
    FileSystem fs = taskOutputPath.getFileSystem(taskContext.getConfiguration());
    return fs.exists(taskOutputPath);
  }

  /**
   * {@inheritDoc}
   * <p/>
   * Merges the completed key-value store under the task's temporary output directory to the
   * key-value store at job output directory, then calls {@link
   * FileOutputCommitter#commitTask(TaskAttemptContext)}.
   */
  @Override
  public void commitTask(TaskAttemptContext taskContext) throws IOException {
    try {
      KEY_VALUE_SYSTEM.mergeStore(KeyValueOutputFormat.getTaskOutputURI(taskContext),
          KeyValueOutputFormat.getJobOutputURI(taskContext));
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
    super.commitTask(taskContext);
  }

  /**
   * {@inheritDoc}
   * <p/>
   * Deletes the completed key-value stores under the task's temporary output directory, and then
   * calls {@link FileOutputCommitter#abortTask(TaskAttemptContext)}.
   */
  @Override
  public void abortTask(TaskAttemptContext taskContext) {
    // TODO(binfan): in Hadoop 1.x FileOutputCommitter#abortTask doesn't throw IOException. To
    // keep the code compile with early Hadoop versions, we catch this exception.
    try {
      try {
        KEY_VALUE_SYSTEM.deleteStore(KeyValueOutputFormat.getTaskOutputURI(taskContext));
      } catch (AlluxioException e) {
        throw new IOException(e);
      }
      super.abortTask(taskContext);
    } catch (IOException e) {
      LOG.error("Failed to abort task", taskContext);
    }
  }

  /**
   * @return the temp directory name
   */
  public static String getPendingDirName() {
    // Due to Hadoop 1 support we stick with the deprecated version. If we drop support for it
    // FileOutputCommitter.PENDING_DIR_NAME will be the new one.
    return FileOutputCommitter.TEMP_DIR_NAME;
  }
}
