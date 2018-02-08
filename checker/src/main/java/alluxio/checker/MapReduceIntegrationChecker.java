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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.URI;

import java.text.SimpleDateFormat;

import java.util.Date;

/**
 * The MapReduce integration checker includes a MapReduce job
 * to test the integration of MapReduce with Alluxio.
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
      + "and fs.AbstractFileSystem.alluxio.impl property "
      + "in core-site.xml file of your Hadoop installation.\n\n"
      + "For details, please refer to: "
      + "https://www.alluxio.org/docs/master/en/Running-Hadoop-MapReduce-on-Alluxio.html\n";
  private static final String TEST_FAILED_MESSAGE = "***** Integration test failed. *****\n";
  private static final String TEST_PASSED_MESSAGE = "***** Integration test passed. *****\n";

  private static Path sInputFilePath;
  private static Path sOutputFilePath;
  private static FileSystem sFileSystem;

  /** The MapReduce task nodes performIntegrationChecks results. */
  protected enum Status {
    FAIL_TO_FIND_CLASS, // MapReduce task nodes cannot recognize Alluxio classes
    FAIL_TO_FIND_FS, // MapReduce task nodes cannot recognize Alluxio filesystem
    SUCCESS;
  }

  /**
   * In each mapper node, we will check whether this node can recognize
   * Alluxio classes and filesystem.
   */
  protected static class CheckStatusMapper extends Mapper<Object, Text, Text, Text> {
    /**
     * Gets the Status and IP address of each mapper task node.
     */
    protected void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      context.write(new Text(performIntegrationChecks().toString()), new Text(getAddress()));
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
   * @return if this current task node can recognize Alluxio classes and filesystem
   */
  private static Status performIntegrationChecks() {
    // Checks if MapReduce nodes can recognize Alluxio classes
    try {
      // Checks if MapReduce task nodes can recognize Alluxio common classes
      Class.forName("alluxio.AlluxioURI");
      // Checks if MapReduce task nodes can recognize Alluxio core client classes
      Class.forName("alluxio.client.file.BaseFileSystem");
      Class.forName("alluxio.hadoop.AlluxioFileSystem");
    } catch (ClassNotFoundException e) {
      LOG.error("Failed to find Alluxio classes on classpath ", e);
      return Status.FAIL_TO_FIND_CLASS;
    }

    // Checks if MapReduce task nodes can recognize Alluxio filesystem
    try {
      FileSystem.getFileSystemClass("alluxio", new Configuration());
    } catch (Exception e) {
      LOG.error("Failed to find Alluxio filesystem ", e);
      return Status.FAIL_TO_FIND_FS;
    }

    return Status.SUCCESS;
  }

  /**
   * @return the current task node IP address
   */
  private static String getAddress() {
    String address;
    try {
      address = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      LOG.debug("MapReduce cannot get host address of current MapReduce node.");
      address = "unknown address";
    }
    return address;
  }

  /**
   * Creates the Hdfs filesystem to generate input and output files.
   *
   * @param conf Hadoop configuration
   */
  private static void createHdfsFilesystem(Configuration conf) throws Exception {
    // Inits HDFS File System Object
    String systemUserName = System.getProperty("user.name");
    System.setProperty("HADOOP_USER_NAME", systemUserName);
    System.setProperty("hadoop.home.dir", "/");
    sFileSystem = FileSystem.get(URI.create(conf.get("fs.defaultFS")), conf);

    String stringPath = "/user/" + systemUserName + "/";
    Path userPath = new Path(stringPath);

    // Creates user folder if not exists
    if (!sFileSystem.exists(userPath)) {
      sFileSystem.mkdirs(userPath);
      LOG.info("User Path " + stringPath + " created.");
    }
    sInputFilePath = new Path(userPath + "/MapReduceInputFile");
    sOutputFilePath = new Path(userPath + "/MapReduceOutputFile");
  }

  /**
   * Creates the MapReduce input file and delete previous output file if exists.
   */
  private static void createInputFile(int inputSplits) throws Exception {
    if (!sFileSystem.exists(new Path(sInputFilePath + "/1.txt"))) {
      LOG.info("Begin Write MapReduce input file into hdfs");
      for (int i = 1; i <= inputSplits; i++) {
        FSDataOutputStream inputFileStream = sFileSystem
            .create(new Path(sInputFilePath + "/" + i + ".txt"));
        inputFileStream.writeByte(1);
        inputFileStream.close();
      }
      LOG.info("End Write MapReduce input file into hdfs");
    }

    if (sFileSystem.exists(sOutputFilePath)) {
      sFileSystem.delete(sOutputFilePath, true);
    }
  }

  /**
   * @return whether MapReduce can find Alluxio classes and filesystem
   */
  private static boolean generateReport() throws Exception {
    // if we do not have output file or MapReduce job is not success, we do not generate report
    if (!sFileSystem.exists(sOutputFilePath)) {
      return false;
    }
    if (!sFileSystem.exists(new Path(sOutputFilePath + "/_SUCCESS"))) {
      return false;
    }

    // Reads all the part-r-* files in MapReduceOutPutFile folder
    sFileSystem.delete(new Path(sOutputFilePath + "/_SUCCESS"), true);
    FileStatus[] outputFileStatus = sFileSystem.listStatus(sOutputFilePath);

    boolean canFindClass = true;
    boolean canFindFS = true;

    StringBuilder cannotFindClassBuilder = new StringBuilder();
    StringBuilder cannotFindFSBuilder = new StringBuilder();
    StringBuilder successBuilder = new StringBuilder();

    for (int i = 0; i < outputFileStatus.length; i++) {
      Path curOutputFilePath = outputFileStatus[i].getPath();
      BufferedReader curOutputFileReader = new BufferedReader(
          new InputStreamReader(sFileSystem.open(curOutputFilePath)));
      try {
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
      } catch (IOException e) {
        LOG.error("Cannot read content from output file: " + e);
      } finally {
        curOutputFileReader.close();
      }
    }

    String cannotFindClassAddresses = cannotFindClassBuilder.toString();
    String cannotFindFSAddresses = cannotFindFSBuilder.toString();
    String successAddresses = successBuilder.toString();

    // Creates a file to save user-facing messages
    File reportFile = new File("./MapReduceIntegrationReport.txt");
    PrintWriter reportWriter = new PrintWriter(reportFile);
    try {
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
    } catch (Exception e) {
      LOG.error("Cannot generate report: " + e);
    } finally {
      reportWriter.flush();
      reportWriter.close();
    }

    return canFindClass && canFindFS;
  }

  /**
   * Implements MapReduce with Alluxio integration checker.
   *
   * @return 0 for success, 1 otherwise
   */
  private static int run(String[] args) throws Exception {
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

    FileInputFormat.addInputPath(job, sInputFilePath);
    FileOutputFormat.setOutputPath(job, sOutputFilePath);

    boolean jobFinished = job.waitForCompletion(true);

    boolean resultIsSuccess = false;
    if (jobFinished) {
      resultIsSuccess = generateReport();
    }

    if (sFileSystem.exists(sInputFilePath)) {
      sFileSystem.delete(sInputFilePath, true);
    }
    if (sFileSystem.exists(sOutputFilePath)) {
      sFileSystem.delete(sOutputFilePath, true);
    }
    sFileSystem.close();

    return (jobFinished && resultIsSuccess) ? 0 : 1;
  }

  /**
   * Main function will be triggered via hadoop jar.
   *
   * @param args inputSplits will be passed in
   */
  public static void main(String[] args) throws Exception {
    System.exit(run(args));
  }
}
