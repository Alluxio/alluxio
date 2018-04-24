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

package alluxio.checker;

import alluxio.checker.CheckerUtils.Status;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A MapReduce job to test the integration of MapReduce with Alluxio.
 * This class will be triggered through hadoop jar.
 *
 * This checker requires a running Hadoop cluster, but does not require a running Alluxio cluster.
 * It will check whether Alluxio classes and Alluxio filesystem can be recognized
 * in Hadoop task nodes.
 */
public class MapReduceIntegrationChecker {
  private static final String FAIL_TO_FIND_CLASS_MESSAGE = "Please distribute "
      + "the Alluxio client jar on the classpath of the application across different nodes.\n\n"
      + "For details, please refer to: "
      + "https://www.alluxio.org/docs/master/en/Running-Hadoop-MapReduce-on-Alluxio.html\n";
  private static final String FAIL_TO_FIND_FS_MESSAGE = "Please check the fs.alluxio.impl "
      + "and fs.AbstractFileSystem.alluxio.impl properties "
      + "in core-site.xml file of your Hadoop installation.\n\n"
      + "For details, please refer to: "
      + "https://www.alluxio.org/docs/master/en/Running-Hadoop-MapReduce-on-Alluxio.html\n";
  private static final String TEST_FAILED_MESSAGE = "***** Integration test failed. *****\n";
  private static final String TEST_PASSED_MESSAGE = "***** Integration test passed. *****\n";

  private Path mOutputFilePath;
  private FileSystem mFileSystem;

  /**
   * An empty input format that can generate input splits to affect the number of mapper nodes.
   */
  static class EmptyInputFormat extends InputFormat<Object, Object> {
    public static boolean sCreateDone = false;

    /**
     * An empty input split.
     */
    static class EmptyInputSplit extends InputSplit implements Writable {

      public EmptyInputSplit() { }

      @Override
      public long getLength() throws IOException {
        return 0;
      }

      @Override
      public String[] getLocations() throws IOException {
        return new String[]{};
      }

      @Override
      public void readFields(DataInput in) throws IOException {
      }

      @Override
      public void write(DataOutput out) throws IOException {
      }
    }

    /**
     * A record reader converting input to record-oriented view.
     */
    static class EmptyRecordReader extends RecordReader<Object, Object> {

      public EmptyRecordReader() {
      }

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) {
      }

      @Override
      public void close() {
      }

      @Override
      public Object getCurrentKey() {
        return new Object();
      }

      @Override
      public Object getCurrentValue() {
        return new Object();
      }

      @Override
      public float getProgress() {
        if (sCreateDone) {
          return 1.0f;
        } else {
          return 0.0f;
        }
      }

      @Override
      public boolean nextKeyValue() {
        if (sCreateDone) {
          return false;
        } else {
          sCreateDone = true;
          return true;
        }
      }
    }

    @Override
    public RecordReader<Object, Object> createRecordReader(InputSplit split,
        TaskAttemptContext context) {
      return new EmptyRecordReader();
    }

    /**
     * Creates the desired number of splits.
     */
    @Override
    public List<InputSplit> getSplits(JobContext job) {
      int numSplits = job.getConfiguration().getInt(MRJobConfig.NUM_MAPS, 10);
      List<InputSplit> splits = new ArrayList<>();
      for (int split = 0; split < numSplits; split++) {
        splits.add(new EmptyInputSplit());
      }
      return splits;
    }
  }

  /**
   * In each mapper node, we will check whether this node can recognize
   * Alluxio classes and filesystem.
   */
  protected static class CheckerMapper extends Mapper<Object, Object, Text, Text> {
    /**
     * Records the Status and IP address of each mapper task node.
     */
    @Override
    protected void map(Object ignoredKey, Object ignoredValue, Context context)
        throws IOException, InterruptedException {
      context.write(new Text(CheckerUtils.performIntegrationChecks().toString()),
          new Text(CheckerUtils.getLocalAddress()));
    }
  }

  /**
   * In each reducer node, we will combine the IP addresses that have the same Status.
   */
  protected static class CheckerReducer extends Reducer<Text, Text, Text, Text> {
    /**
     * Merges the IP addresses of same Status.
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      Set<String> addressSet = new HashSet<>();
      for (Text val : values) {
        addressSet.add(val.toString());
      }
      context.write(key, new Text(String.join(" ", addressSet)));
    }
  }

  /**
   * Creates the HDFS filesystem to store output files.
   *
   * @param conf Hadoop configuration
   */
  private void createHdfsFilesystem(Configuration conf) throws Exception {
    // Inits HDFS file system object
    mFileSystem = FileSystem.get(URI.create(conf.get("fs.defaultFS")), conf);
    mOutputFilePath = new Path("./MapReduceOutputFile");
    if (mFileSystem.exists(mOutputFilePath)) {
      mFileSystem.delete(mOutputFilePath, true);
    }
  }

  /**
   * @return result Status from MapReduce output
   */
  private Status generateReport() throws Exception {
    // Read all the part-r-* files in MapReduceOutPutFile folder
    FileStatus[] outputFileStatus = mFileSystem.listStatus(mOutputFilePath,
        path -> path.getName().startsWith(("part-")));

    Map<Status, List<String>> resultMap = new HashMap<>();
    for (int i = 0; i < outputFileStatus.length; i++) {
      Path curOutputFilePath = outputFileStatus[i].getPath();
      try (BufferedReader curOutputFileReader = new BufferedReader(
          new InputStreamReader(mFileSystem.open(curOutputFilePath)))) {
        String nextLine = "";
        while ((nextLine = curOutputFileReader.readLine()) != null) {
          int sep = nextLine.indexOf("\t");
          Status curStatus = Status.valueOf(nextLine.substring(0, sep).trim());
          String curAddresses = nextLine.substring(sep + 1).trim();
          List<String> addresses = resultMap.getOrDefault(curStatus, new ArrayList<>());
          addresses.add(curAddresses);
          resultMap.put(curStatus, addresses);
        }
      }
    }

    try (PrintWriter reportWriter = CheckerUtils.initReportFile()) {
      Status resultStatus = CheckerUtils.printNodesResults(resultMap, reportWriter);
      switch (resultStatus) {
        case FAIL_TO_FIND_CLASS:
          reportWriter.println(FAIL_TO_FIND_CLASS_MESSAGE);
          reportWriter.println(TEST_FAILED_MESSAGE);
          break;
        case FAIL_TO_FIND_FS:
          reportWriter.println(FAIL_TO_FIND_FS_MESSAGE);
          reportWriter.println(TEST_FAILED_MESSAGE);
          break;
        default:
          reportWriter.println(TEST_PASSED_MESSAGE);
      }
      reportWriter.flush();
      return resultStatus;
    }
  }

  /**
   * Implements MapReduce with Alluxio integration checker.
   *
   * @return 0 for success, 2 for unable to find Alluxio classes, 1 otherwise
   */
  private int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String numMaps =  new GenericOptionsParser(conf, args).getRemainingArgs()[0];
    conf.set(MRJobConfig.NUM_MAPS, numMaps);
    createHdfsFilesystem(conf);

    Job job = Job.getInstance(conf, "MapReduceIntegrationChecker");
    job.setJarByClass(MapReduceIntegrationChecker.class);
    job.setMapperClass(CheckerMapper.class);
    job.setCombinerClass(CheckerReducer.class);
    job.setReducerClass(CheckerReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(EmptyInputFormat.class);
    FileOutputFormat.setOutputPath(job, mOutputFilePath);

    try {
      if (!job.waitForCompletion(true)) {
        return 1;
      }
      Status resultStatus = generateReport();
      return resultStatus.equals(Status.SUCCESS) ? 0
          : (resultStatus.equals(Status.FAIL_TO_FIND_CLASS) ? 2 : 1);
    } finally {
      if (mFileSystem.exists(mOutputFilePath)) {
        mFileSystem.delete(mOutputFilePath, true);
      }
      mFileSystem.close();
    }
  }

  /**
   * Main function will be triggered via hadoop jar.
   *
   * @param args numMaps will be passed in
   */
  public static void main(String[] args) throws Exception {
    MapReduceIntegrationChecker checker = new MapReduceIntegrationChecker();
    System.exit(checker.run(args));
  }
}
