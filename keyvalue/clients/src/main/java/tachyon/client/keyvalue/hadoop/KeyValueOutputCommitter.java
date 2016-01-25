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

package tachyon.client.keyvalue.hadoop;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.TaskAttemptContext;

import com.google.common.collect.Lists;

import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.client.keyvalue.KeyValueStores;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.TachyonException;

/**
 * Extension of {@link FileOutputCommitter} where creating, completing, or deleting a
 * {@link KeyValueStores} in different phases of a job's or task's lifecycle is considered.
 * <p>
 * This committer must be used along with {@link KeyValueOutputFormat} to merge the key-value stores
 * created by each Reducer into one key-value store under the MapReduce output directory.
 */
@PublicApi
public final class KeyValueOutputCommitter extends FileOutputCommitter {
  private static final KeyValueStores KEY_VALUE_STORES = KeyValueStores.Factory.create();

  private List<TachyonURI> getTaskTemporaryStores(JobConf conf) throws IOException {
    TachyonURI taskOutputURI = KeyValueOutputFormat.getTaskOutputURI(conf);
    Path outputPath = FileOutputFormat.getOutputPath(conf);
    FileSystem fs = outputPath.getFileSystem(conf);
    FileStatus[] subDirs = fs.listStatus(new Path(taskOutputURI.toString()));
    List<TachyonURI> ret = Lists.newArrayListWithExpectedSize(subDirs.length);
    for (FileStatus subDir : subDirs) {
      ret.add(taskOutputURI.join(subDir.getPath().getName()));
    }
    return ret;
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
    TachyonURI jobOutputURI = KeyValueOutputFormat.getJobOutputURI(conf);
    for (TachyonURI tempStoreUri : getTaskTemporaryStores(conf)) {
      try {
        KEY_VALUE_STORES.merge(tempStoreUri, jobOutputURI);
      } catch (TachyonException e) {
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
    for (TachyonURI tempStoreUri : getTaskTemporaryStores(context.getJobConf())) {
      try {
        KEY_VALUE_STORES.delete(tempStoreUri);
      } catch (FileDoesNotExistException e) {
        // The goal of deleting the store is to cleanup directories before aborting the task, since
        // the store directory does not exist, it meets the goal, nothing needs to be done.
      } catch (TachyonException e) {
        throw new IOException(e);
      }
    }
    super.abortTask(context);
  }
}
