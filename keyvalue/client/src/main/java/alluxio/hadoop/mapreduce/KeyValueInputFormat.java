/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
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
import alluxio.client.ClientContext;
import alluxio.client.keyvalue.KeyValueMasterClient;
import alluxio.client.keyvalue.KeyValueSystem;
import alluxio.exception.AlluxioException;
import alluxio.thrift.PartitionInfo;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Implementation of {@code org.apache.hadoop.mapred.InputFormat} for MapReduce programs to access
 * {@link KeyValueSystem}.
 * <p>
 * It takes a {@link KeyValueSystem} URI, and emits key-value pairs stored in the KeyValueStore to
 * {@code org.apache.hadoop.mapred.Mapper}s.
 */
@PublicApi
@ThreadSafe
public final class KeyValueInputFormat extends InputFormat<BytesWritable, BytesWritable> {
  private final KeyValueMasterClient mKeyValueMasterClient =
      new KeyValueMasterClient(ClientContext.getMasterAddress(), ClientContext.getConf());

  /**
   * Returns a list of {@link KeyValueInputSplit} where each split is one key-value partition.
   *
   * @param jobContext MapReduce job configuration
   * @return list of {@link InputSplit}s, each split is a partition
   * @throws IOException if information about the partition cannot be retrieved
   */
  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
    // The paths are MapReduce program's inputs specified in
    // {@code mapreduce.input.fileinputformat.inputdir}, each path should be a key-value store.
    Path[] paths = FileInputFormat.getInputPaths(jobContext);
    List<InputSplit> splits = Lists.newArrayList();
    try {
      for (Path path : paths) {
        List<PartitionInfo> partitionInfos =
            mKeyValueMasterClient.getPartitionInfo(new AlluxioURI(path.toString()));
        for (PartitionInfo partitionInfo : partitionInfos) {
          splits.add(new KeyValueInputSplit(partitionInfo));
        }
      }
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
    return splits;
  }

  @Override
  public RecordReader<BytesWritable, BytesWritable> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext taskContext) throws IOException {
    return new KeyValueRecordReader();
  }
}
