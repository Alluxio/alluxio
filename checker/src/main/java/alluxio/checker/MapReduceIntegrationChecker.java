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

import alluxio.checker.CommonCheckerUtils.Status;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

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

  private Path mInputFilePath;
  private Path mOutputFilePath;
  private FileSystem mFileSystem;

  /**
   * In each mapper node, we will check whether this node can recognize
   * Alluxio classes and filesystem.
   */
  protected static class CheckStatusMapper extends Mapper<Object, Text, Text, Text> {
    /**
     * Records the Status and IP address of each mapper task node.
     */
    protected void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      context.write(new Text(CommonCheckerUtils.performIntegrationChecks().toString()),
          new Text(CommonCheckerUtils.getAddress()));
    }
  }

  /**
   * In each reducer node, we will combine the IP addresses that have the same Status.
   */
  protected static class MergeStatusReducer extends Reducer<Text, Text, Text, Text> {
    private Text mMergedAddress = new Text();

    /**
     * Merges the IP addresses of same Status.
     */
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      String mergedAddress = "";
      for (Text val : values) {
        String nodeAddress = val.toString();
        if (!mergedAddress.contains(nodeAddress)) {
          mergedAddress = mergedAddress + " " + nodeAddress;
        }
      }
      mMergedAddress.set(mergedAddress);
      context.write(key, mMergedAddress);
    }
  }

  /**
   * Creates the HDFS filesystem to generate input and output files.
   *
   * @param conf Hadoop configuration
   */
  private void createHdfsFilesystem(Configuration conf) throws Exception {
    // Inits HDFS File System Object
    String systemUserName = System.getProperty("user.name");
    System.setProperty("HADOOP_USER_NAME", systemUserName);
    System.setProperty("hadoop.home.dir", "/");
    mFileSystem = FileSystem.get(URI.create(conf.get("fs.defaultFS")), conf);

    String stringPath = "/user/" + systemUserName + "/";
    Path userPath = new Path(stringPath);

    // Creates user folder if not exists
    if (!mFileSystem.exists(userPath)) {
      mFileSystem.mkdirs(userPath);
      LOG.info("User Path " + stringPath + " created.");
    }
    mInputFilePath = new Path(userPath + "/MapReduceInputFile");
    mOutputFilePath = new Path(userPath + "/MapReduceOutputFile");
  }

  /**
   * Creates the MapReduce input file and delete previous output file if exists.
   */
  private void createInputFile(int inputSplits) throws Exception {
    if (!mFileSystem.exists(new Path(mInputFilePath + "/1.txt"))) {
      LOG.info("Begin Write MapReduce input file into HDFS");
      for (int i = 1; i <= inputSplits; i++) {
        FSDataOutputStream inputFileStream = mFileSystem
            .create(new Path(mInputFilePath + "/" + i + ".txt"));
        inputFileStream.writeByte(1);
        inputFileStream.close();
      }
      LOG.info("End Write MapReduce input file into HDFS");
    }

    if (mFileSystem.exists(mOutputFilePath)) {
      mFileSystem.delete(mOutputFilePath, true);
    }
  }

  /**
   * @return result Status summarization
   */
  private Status generateReport() throws Exception {
    // Reads all the part-r-* files in MapReduceOutPutFile folder
    mFileSystem.delete(new Path(mOutputFilePath + "/_SUCCESS"), true);
    FileStatus[] outputFileStatus = mFileSystem.listStatus(mOutputFilePath);

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
              .substring(0, nextLine.indexOf(' ')).trim());
          String curOutputAddresses = nextLine.substring(nextLine.indexOf(' ') + 1).trim();
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
   * Cleans the HDFS input and output files.
   */
  private void cleanFileSystem() throws Exception {
    if (mFileSystem.exists(mInputFilePath)) {
      mFileSystem.delete(mInputFilePath, true);
    }
    if (mFileSystem.exists(mOutputFilePath)) {
      mFileSystem.delete(mOutputFilePath, true);
    }
    mFileSystem.close();
  }

  /**
   * Implements MapReduce with Alluxio integration checker.
   *
   * @return 0 for success, 2 for unable to find Alluxio classes, 1 otherwise
   */
  private int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs =  new GenericOptionsParser(conf, args).getRemainingArgs();

    createHdfsFilesystem(conf);
    int inputSplits = Integer.parseInt(otherArgs[0]);
    createInputFile(inputSplits);

    Job job = Job.getInstance(conf, "MapReduceIntegrationChecker");
    job.setJarByClass(MapReduceIntegrationChecker.class);
    job.setMapperClass(CheckStatusMapper.class);
    job.setCombinerClass(MergeStatusReducer.class);
    job.setReducerClass(MergeStatusReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, mInputFilePath);
    FileOutputFormat.setOutputPath(job, mOutputFilePath);

    if (!job.waitForCompletion(true)) {
      cleanFileSystem();
      return 1;
    }

    Status resultStatus = generateReport();
    cleanFileSystem();
    return resultStatus.equals(Status.SUCCESS) ? 0
        : (resultStatus.equals(Status.FAIL_TO_FIND_CLASS) ? 2 : 1);
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
