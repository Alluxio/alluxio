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

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.TaskAttemptContext;

import com.google.common.collect.Lists;

import alluxio.AlluxioURI;
import alluxio.annotation.PublicApi;
import alluxio.client.keyvalue.KeyValueSystem;
import alluxio.exception.AlluxioException;

/**
 * Extension of {@link FileOutputCommitter} where creating, completing, or deleting a
 * {@link KeyValueSystem} in different phases of a job's or task's lifecycle is considered.
 * <p>
 * This committer must be used along with {@link KeyValueOutputFormat} to merge the key-value stores
 * created by each Reducer into one key-value store under the MapReduce output directory.
 */
@PublicApi
@ThreadSafe
public final class KeyValueOutputCommitter extends FileOutputCommitter {
  private static final KeyValueSystem KEY_VALUE_SYSTEM = KeyValueSystem.Factory.create();

  private List<AlluxioURI> getTaskTemporaryStores(JobConf conf) throws IOException {
    AlluxioURI taskOutputURI = KeyValueOutputFormat.getTaskOutputURI(conf);
    Path taskOutputPath = new Path(taskOutputURI.toString());
    FileSystem fs = taskOutputPath.getFileSystem(conf);
    FileStatus[] subDirs = fs.listStatus(taskOutputPath);
    List<AlluxioURI> temporaryStores = Lists.newArrayListWithExpectedSize(subDirs.length);
    for (FileStatus subDir : subDirs) {
      temporaryStores.add(taskOutputURI.join(subDir.getPath().getName()));
    }
    return temporaryStores;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Merges the completed key-value stores under the task's temporary output directory to the
   * key-value store created in {@link #setupJob(JobContext)}, then calls
   * {@link FileOutputCommitter#commitTask(TaskAttemptContext)}.
   */
  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    JobConf conf = context.getJobConf();
    AlluxioURI jobOutputURI = KeyValueOutputFormat.getJobOutputURI(conf);
    for (AlluxioURI tempStoreUri : getTaskTemporaryStores(conf)) {
      try {
        KEY_VALUE_SYSTEM.mergeStore(tempStoreUri, jobOutputURI);
      } catch (AlluxioException e) {
        throw new IOException(e);
      }
    }
    super.commitTask(context);
  }

  /**
   * {@inheritDoc}
   * <p>
   * Deletes the completed key-value stores under the task's temporary output directory, and then
   * calls {@link FileOutputCommitter#abortTask(TaskAttemptContext)}.
   */
  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    for (AlluxioURI tempStoreUri : getTaskTemporaryStores(context.getJobConf())) {
      try {
        KEY_VALUE_SYSTEM.deleteStore(tempStoreUri);
      } catch (AlluxioException e) {
        throw new IOException(e);
      }
    }
    super.abortTask(context);
  }
}
