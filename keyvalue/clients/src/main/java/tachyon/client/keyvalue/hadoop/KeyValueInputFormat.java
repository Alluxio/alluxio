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

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.google.common.collect.Lists;

import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.client.ClientContext;
import tachyon.client.keyvalue.KeyValueMasterClient;
import tachyon.exception.TachyonException;
import tachyon.thrift.PartitionInfo;

/**
 * Implementation of {@link org.apache.hadoop.mapred.InputFormat} for MapReduce programs to access
 * {@link tachyon.client.keyvalue.KeyValueStores}.
 * <p>
 * It takes a {@link tachyon.client.keyvalue.KeyValueStores} URI, and emits key-value pairs stored
 * in the KeyValueStore to {@link org.apache.hadoop.mapred.Mapper}s.
 */
@PublicApi
@NotThreadSafe
public final class KeyValueInputFormat implements InputFormat {
  private final KeyValueMasterClient mKeyValueMasterClient =
      new KeyValueMasterClient(ClientContext.getMasterAddress(), ClientContext.getConf());

  /**
   * Returns each partition as a {@link KeyValueInputSplit}.
   *
   * @param conf MapReduce job configuration
   * @param numSplits number of splits, ignored because it is determined by number of partitions
   * @return list of {@link InputSplit}s, each split is a partition
   * @throws IOException if information about the partition cannot be retrieved
   */
  @Override
  public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
    // The paths are MapReduce program's inputs specified in
    // {@code mapreduce.input.fileinputformat.inputdir}, each path should be a key-value store.
    Path[] paths = FileInputFormat.getInputPaths(conf);
    List<InputSplit> splits = Lists.newArrayList();
    try {
      for (Path path : paths) {
        List<PartitionInfo> partitionInfos =
            mKeyValueMasterClient.getPartitionInfo(new TachyonURI(path.toString()));
        for (PartitionInfo partitionInfo : partitionInfos) {
          splits.add(new KeyValueInputSplit(partitionInfo));
        }
      }
    } catch (TachyonException e) {
      throw new IOException(e);
    }
    InputSplit[] ret = new InputSplit[splits.size()];
    return splits.toArray(ret);
  }

  @Override
  public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter)
      throws IOException {
    if (inputSplit instanceof KeyValueInputSplit) {
      try {
        return new KeyValueRecordReader((KeyValueInputSplit) inputSplit);
      } catch (TachyonException e) {
        throw new IOException(e);
      }
    } else {
      throw new IOException("Expected InputSplit to be instance of KeyValueInputSplit.");
    }
  }
}
