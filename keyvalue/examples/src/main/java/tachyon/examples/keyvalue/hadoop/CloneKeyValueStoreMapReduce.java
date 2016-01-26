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

package tachyon.examples.keyvalue.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import tachyon.client.keyvalue.hadoop.KeyValueInputFormat;
import tachyon.client.keyvalue.hadoop.KeyValueOutputCommitter;
import tachyon.client.keyvalue.hadoop.KeyValueOutputFormat;

/**
 * This boring MapReduce job clones a key-value store to a different URI.
 */
public final class CloneKeyValueStoreMapReduce {
  /**
   * The mapper emits all key-value pairs it receives to reducers.
   */
  public static class Map extends MapReduceBase
      implements Mapper<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
    @Override
    public void map(BytesWritable key, BytesWritable value,
        OutputCollector<BytesWritable, BytesWritable> out, Reporter reporter) throws IOException {
      out.collect(key, value);
    }
  }

  /**
   * The reducer writes all key-value pairs it receives to the new key-value store.
   */
  public static class Reduce extends MapReduceBase
      implements Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
    @Override
    public void reduce(BytesWritable key, Iterator<BytesWritable> values,
        OutputCollector<BytesWritable, BytesWritable> out, Reporter reporter) throws IOException {
      while (values.hasNext()) {
        out.collect(key, values.next());
      }
    }
  }

  /**
   * @param args two parameters, the first is the input key-value store path, the second is the
   *    output key-value store path
   * @throws Exception if any exception happens
   */
  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(CloneKeyValueStoreMapReduce.class);
    conf.setJobName("clone key-value store");

    conf.setOutputKeyClass(BytesWritable.class);
    conf.setOutputValueClass(BytesWritable.class);

    conf.setMapperClass(Map.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(KeyValueInputFormat.class);
    conf.setOutputFormat(KeyValueOutputFormat.class);
    conf.setOutputCommitter(KeyValueOutputCommitter.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}
