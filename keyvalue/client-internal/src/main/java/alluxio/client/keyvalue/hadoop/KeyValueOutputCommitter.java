/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.client.keyvalue.hadoop;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.client.keyvalue.KeyValueSystem;
import alluxio.exception.AlluxioException;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Extension of {@link FileOutputCommitter} where creating, completing, or deleting a {@link
 * KeyValueSystem} in different phases of a job's or task's lifecycle is considered.
 * <p/>
 * This committer must be used along with {@link KeyValueOutputFormat} to merge the key-value stores
 * created by each Reducer into one key-value store under the MapReduce output directory.
 */
@PublicApi
@ThreadSafe
public final class KeyValueOutputCommitter extends FileOutputCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final KeyValueSystem KEY_VALUE_SYSTEM = KeyValueSystem.Factory.create();

  public KeyValueOutputCommitter(Path outputPath, TaskAttemptContext taskContext)
      throws IOException {
    super(outputPath, taskContext);
  }

  private List<AlluxioURI> getTaskTemporaryStores(TaskAttemptContext taskContext)
      throws IOException {
    AlluxioURI taskOutputURI = KeyValueOutputFormat.getTaskOutputURI(taskContext);
    Path taskOutputPath = new Path(taskOutputURI.toString());
    FileSystem fs = taskOutputPath.getFileSystem(taskContext.getConfiguration());
    FileStatus[] subDirs = fs.listStatus(taskOutputPath);
    List<AlluxioURI> temporaryStores = Lists.newArrayListWithExpectedSize(subDirs.length);
    for (FileStatus subDir : subDirs) {
      temporaryStores.add(taskOutputURI.join(subDir.getPath().getName()));
    }
    return temporaryStores;
  }

  /**
   * {@inheritDoc}
   * <p/>
   * Merges the completed key-value stores under the task's temporary output directory to the
   * key-value store created in {@link #setupJob(JobContext)}, then calls {@link
   * FileOutputCommitter#commitTask(TaskAttemptContext)}.
   */
  @Override
  public void commitTask(TaskAttemptContext taskContext) throws IOException {
    AlluxioURI jobOutputURI = KeyValueOutputFormat.getJobOutputURI(taskContext);
    for (AlluxioURI tempStoreUri : getTaskTemporaryStores(taskContext)) {
      try {
        KEY_VALUE_SYSTEM.mergeStore(tempStoreUri, jobOutputURI);
      } catch (AlluxioException e) {
        throw new IOException(e);
      }
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
      for (AlluxioURI tempStoreUri : getTaskTemporaryStores(taskContext)) {
        try {
          KEY_VALUE_SYSTEM.deleteStore(tempStoreUri);
        } catch (AlluxioException e) {
          throw new IOException(e);
        }
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
    return FileOutputCommitter.TEMP_DIR_NAME;
  }
}
