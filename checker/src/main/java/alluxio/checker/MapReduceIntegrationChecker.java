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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
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
  private static final Logger LOG = LoggerFactory.getLogger(MapReduceIntegrationChecker.class);
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
   * An input format that assigns an integer to each mapper.
   */
  static class CheckerInputFormat
      extends InputFormat<IntWritable, NullWritable> {
    public static boolean sCreateDone = false;
    /**
     * An input split consisting of an integer 1.
     */
    static class CheckerInputSplit extends InputSplit implements Writable {
      int mValue;

      public CheckerInputSplit() { }

      public CheckerInputSplit(int inputValue) {
        mValue = inputValue;
      }

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
        mValue = WritableUtils.readVInt(in);
      }

      @Override
      public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, mValue);
      }
    }

    /**
     * A record reader that will generate an integer.
     */
    static class CheckerRecordReader
        extends RecordReader<IntWritable, NullWritable> {
      int mIntegerVal;
      IntWritable mKey = null;

      public CheckerRecordReader() {
      }

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) {
        mIntegerVal = ((CheckerInputSplit) split).mValue;
      }

      @Override
      public void close() {
        // NOTHING
      }

      @Override
      public IntWritable getCurrentKey() {
        return mKey;
      }

      @Override
      public NullWritable getCurrentValue() {
        return NullWritable.get();
      }

      @Override
      public float getProgress() throws IOException {
        if (sCreateDone) {
          return 1.0f;
        } else {
          return 0.0f;
        }
      }

      @Override
      public boolean nextKeyValue() {
        if (mKey == null) {
          mKey = new IntWritable(mIntegerVal);
        }
        if (sCreateDone) {
          return false;
        } else {
          sCreateDone = true;
          return true;
        }
      }
    }

    @Override
    public RecordReader<IntWritable, NullWritable> createRecordReader(InputSplit split,
        TaskAttemptContext context) {
      return new CheckerRecordReader();
    }

    /**
     * Creates the desired number of splits.
     */
    @Override
    public List<InputSplit> getSplits(JobContext job) {
      int numSplits = job.getConfiguration().getInt(MRJobConfig.NUM_MAPS, 1);
      List<InputSplit> splits = new ArrayList<>();
      for (int split = 0; split < numSplits; split++) {
        splits.add(new CheckerInputSplit(1));
      }
      return splits;
    }
  }

  /**
   * In each mapper node, we will check whether this node can recognize
   * Alluxio classes and filesystem.
   */
  protected static class CheckStatusMapper extends Mapper<IntWritable, NullWritable, Text, Text> {
    /**
     * Records the Status and IP address of each mapper task node.
     */
    @Override
    protected void map(IntWritable key, NullWritable value, Context context)
        throws IOException, InterruptedException {
      context.write(new Text(CheckerUtils.performIntegrationChecks().toString()),
          new Text(CheckerUtils.getLocalAddress()));
    }
  }

  /**
   * In each reducer node, we will combine the IP addresses that have the same Status.
   */
  protected static class MergeStatusReducer extends Reducer<Text, Text, Text, Text> {
    /**
     * Merges the IP addresses of same Status.
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      Set<String> addressSet = new HashSet<>();
      for (Text val : values) {
        String curAddress = val.toString();
        if (!addressSet.contains(curAddress)) {
          addressSet.add(curAddress);
        }
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
    // Inits HDFS File System Object
    mFileSystem = FileSystem.get(URI.create(conf.get("fs.defaultFS")), conf);
    mOutputFilePath = new Path("./MapReduceOutputFile");
    if (mFileSystem.exists(mOutputFilePath)) {
      mFileSystem.delete(mOutputFilePath, true);
    }
  }

  /**
   * @return result Status summarization
   */
  private Status generateReport() throws Exception {
    // Reads all the part-r-* files in MapReduceOutPutFile folder
    FileStatus[] outputFileStatus = mFileSystem.listStatus(mOutputFilePath,
        path -> path.getName().startsWith(("part-")));

    boolean canFindClass = true;
    boolean canFindFS = true;

    StringBuilder cannotFindClassBuilder = new StringBuilder();
    StringBuilder cannotFindFSBuilder = new StringBuilder();
    StringBuilder successBuilder = new StringBuilder();

    for (int i = 0; i < outputFileStatus.length; i++) {
      Path curOutputFilePath = outputFileStatus[i].getPath();
      try (BufferedReader curOutputFileReader = new BufferedReader(
          new InputStreamReader(mFileSystem.open(curOutputFilePath)))) {
        String nextLine = "";
        while ((nextLine = curOutputFileReader.readLine()) != null) {
          Status curOutputStatus = Status.valueOf(nextLine
              .substring(0, nextLine.indexOf(";")).trim());
          String curOutputAddresses = nextLine.substring(nextLine.indexOf(";") + 1).trim();
          switch (curOutputStatus) {
            case FAIL_TO_FIND_CLASS:
              canFindClass = false;
              cannotFindClassBuilder.append(" ").append(curOutputAddresses);
              break;
            case FAIL_TO_FIND_FS:
              canFindFS = false;
              cannotFindFSBuilder.append(" ").append(curOutputAddresses);
              break;
            default:
              successBuilder.append(" ").append(curOutputAddresses);
          }
        }
      }
    }

    String cannotFindClassAddresses = cannotFindClassBuilder.toString();
    String cannotFindFSAddresses = cannotFindFSBuilder.toString();
    String successAddresses = successBuilder.toString();

    // Creates a file to save user-facing messages
    File reportFile = new File("./MapReduceIntegrationReport.txt");
    try (PrintWriter reportWriter = new PrintWriter(reportFile)) {
      SimpleDateFormat df = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
      Date date = new Date();
      reportWriter.printf("%n%n%n***** The integration checker ran at %s. *****%n%n",
          df.format(date));

      if (!cannotFindClassAddresses.equals("")) {
        reportWriter.printf("Hadoop nodes of IP addresses: %s "
            + "cannot recognize Alluxio classes.%n%n", cannotFindClassAddresses);
      }
      if (!cannotFindFSAddresses.equals("")) {
        reportWriter.printf("Hadoop nodes of IP addresses: %s "
            + "cannot recognize Alluxio filesystem.%n%n", cannotFindFSAddresses);
      }
      if (!successAddresses.equals("")) {
        reportWriter.printf("Hadoop nodes of IP addresses: %s "
            + "can recognize Alluxio filesystem.%n%n", successAddresses);
      }

      if (!canFindClass) {
        reportWriter.println(FAIL_TO_FIND_CLASS_MESSAGE);
        reportWriter.println(TEST_FAILED_MESSAGE);
      } else if (!canFindFS) {
        reportWriter.println(FAIL_TO_FIND_FS_MESSAGE);
        reportWriter.println(TEST_FAILED_MESSAGE);
      } else {
        reportWriter.println(TEST_PASSED_MESSAGE);
      }
    }

    return canFindClass ? (canFindFS ? Status.SUCCESS : Status.FAIL_TO_FIND_FS)
        : Status.FAIL_TO_FIND_CLASS;
  }

  /**
   * Implements MapReduce with Alluxio integration checker.
   *
   * @return 0 for success, 2 for unable to find Alluxio classes, 1 otherwise
   */
  private int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs =  new GenericOptionsParser(conf, args).getRemainingArgs();
    conf.set(MRJobConfig.NUM_MAPS, otherArgs[0]);
    conf.set("mapred.textoutputformat.separator", ";"); // Supports Hadoop 1.x
    conf.set("mapreduce.textoutputformat.separator", ";"); // Supports Hadoop 2.x
    createHdfsFilesystem(conf);

    Job job = Job.getInstance(conf, "MapReduceIntegrationChecker");
    job.setJarByClass(MapReduceIntegrationChecker.class);
    job.setMapperClass(CheckStatusMapper.class);
    job.setCombinerClass(MergeStatusReducer.class);
    job.setReducerClass(MergeStatusReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(CheckerInputFormat.class);
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
   * @param args inputSplits will be passed in
   */
  public static void main(String[] args) throws Exception {
    MapReduceIntegrationChecker checker = new MapReduceIntegrationChecker();
    System.exit(checker.run(args));
  }
}
