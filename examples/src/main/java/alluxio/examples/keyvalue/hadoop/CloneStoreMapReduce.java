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

package alluxio.examples.keyvalue.hadoop;

import alluxio.hadoop.mapreduce.KeyValueInputFormat;
import alluxio.hadoop.mapreduce.KeyValueOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * This MapReduce job reads a key-value store and saves the same key-value data to another
 * key-value store with a different URI.
 */
public final class CloneStoreMapReduce {
  /**
   * The mapper emits all key-value pairs it receives to reducers.
   */
  public static class CloneStoreMapper
      extends Mapper<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {

    public void map(BytesWritable key, BytesWritable value, Context context)
        throws IOException, InterruptedException {
      context.write(key, value);
    }
  }

  /**
   * The reducer writes all key-value pairs it receives to the new key-value store.
   */
  public static class CloneStoreReducer
      extends Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {

    public void reduce(BytesWritable key, Iterable<BytesWritable> values, Context context)
        throws IOException, InterruptedException {
      for (BytesWritable value : values) {
        context.write(key, value);
      }
    }
  }

  /**
   * @param args two parameters, the first is the input key-value store path, the second is the
   *    output key-value store path
   * @throws Exception if any exception happens
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    // NOTE(binfan): we are using the deprecated constructor of Job instance to compile with
    // hadoop-1.0. If this is not a concern, a better way is
    //     Job job = Job.getInstance(conf);
    Job job = new Job(conf);

    job.setJobName("CloneStoreMapReduce");
    job.setJarByClass(CloneStoreMapReduce.class);

    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(BytesWritable.class);

    job.setMapperClass(CloneStoreMapper.class);
    job.setReducerClass(CloneStoreReducer.class);

    job.setInputFormatClass(KeyValueInputFormat.class);
    job.setOutputFormatClass(KeyValueOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
