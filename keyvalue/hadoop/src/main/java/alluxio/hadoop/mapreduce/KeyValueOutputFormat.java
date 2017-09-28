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

import alluxio.AlluxioURI;
import alluxio.annotation.PublicApi;
import alluxio.client.keyvalue.KeyValueSystem;
import alluxio.exception.AlluxioException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
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
   * Constructs a new {@link KeyValueOutputFormat}.
   */
  public KeyValueOutputFormat() {}

  /**
   * @param taskContext MapReduce task context
   * @return the task's temporary output path ${job output directory}/_temporary/${task attempt id}
   */
  public static AlluxioURI getTaskOutputURI(TaskAttemptContext taskContext) {
    return getJobOutputURI(taskContext).join(KeyValueOutputCommitter.getPendingDirName()).join(
        taskContext.getTaskAttemptID().toString());
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
   * This method is called immediately when job is submitted, a key-value store is created at the
   * job's output directory, key-value stores created by MapReduce tasks will be merged into this
   * store when task is submitted.
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

  /**
   * @param taskContext MapReduce task configuration
   * @return a {@link KeyValueOutputCommitter}
   */
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskContext) throws IOException {
    if (mCommitter == null) {
      mCommitter = new KeyValueOutputCommitter(new Path(KeyValueOutputFormat.getJobOutputURI(
          taskContext).toString()), taskContext);
    }
    return mCommitter;
  }
}
