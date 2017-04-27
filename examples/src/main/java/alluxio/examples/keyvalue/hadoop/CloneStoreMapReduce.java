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
   * Constructs a new {@link CloneStoreMapReduce}.
   */
  public CloneStoreMapReduce() {}

  /**
   * The mapper emits all key-value pairs it receives to reducers.
   */
  public static class CloneStoreMapper
      extends Mapper<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {

    /**
     * Constructs a new {@link CloneStoreMapper}.
     */
    public CloneStoreMapper() {}

    @Override
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

    /**
     * Constructs a new {@link CloneStoreReducer}.
     */
    public CloneStoreReducer() {}

    @Override
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

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
