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

package alluxio.hadoop.mapreduce;

import alluxio.AlluxioURI;
import alluxio.annotation.PublicApi;
import alluxio.client.keyvalue.KeyValueSystem;
import alluxio.exception.AlluxioException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * An {@link OutputFormat} implementation to let MapReduce job write to a key-value store under the
 * MapReduce output directory.
 * <p>
 * There are different key-value stores under the output directory configured via MapReduce property
 * {@code mapreduce.output.fileoutputformat.outputdir} for different tasks. The stores are merged
 * into one key-value store under the MapReduce output directory by {@link
 * KeyValueOutputCommitter}.
 */
// TODO(cc): Consider key distributions in each Reducer.
@PublicApi
@ThreadSafe
public final class KeyValueOutputFormat extends FileOutputFormat<BytesWritable, BytesWritable> {
  private OutputCommitter mCommitter;

  /**
   * @param taskContext MapReduce task context
   * @return the task's temporary output path ${mapred.out.dir}/_temporary/_${taskid}
   */
  public static AlluxioURI getTaskOutputURI(TaskAttemptContext taskContext) {
    int taskId = taskContext.getTaskAttemptID().getTaskID().getId();
    return getJobOutputURI(taskContext).join(KeyValueOutputCommitter.getPendingDirName())
        .join("_" + TaskAttemptID.forName(String.valueOf(taskId)));
  }

  /**
   * @param jobContext MapReduce job configuration
   * @return the job's output path
   */
  public static AlluxioURI getJobOutputURI(JobContext jobContext) {
    return new AlluxioURI(FileOutputFormat.getOutputPath(jobContext).toString());
  }

  @Override
  public RecordWriter<BytesWritable, BytesWritable> getRecordWriter(
      TaskAttemptContext taskAttemptContext) throws IOException {
    return new KeyValueRecordWriter(getTaskOutputURI(taskAttemptContext));
  }

  /**
   * {@inheritDoc}
   * <p>
   * {@link KeyValueOutputCommitter} is forced to be used. If the output path exists, an exception
   * is thrown, otherwise, an empty key-value store is created at the output path.
   * <p>
   * NOTE: This method is called immediately when job is submitted, so that modifications to the
   * {@link JobContext} are reflected in the whole job.
   */
  @Override
  public void checkOutputSpecs(JobContext jobContext) throws IOException {
    super.checkOutputSpecs(jobContext);
    try {
      KeyValueSystem.Factory.create().createStore(KeyValueOutputFormat.getJobOutputURI(jobContext))
          .close();
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskContext) throws IOException {
    if (mCommitter == null) {
      mCommitter =
          new KeyValueOutputCommitter(FileOutputFormat.getOutputPath(taskContext), taskContext);
    }
    return mCommitter;
  }
}
