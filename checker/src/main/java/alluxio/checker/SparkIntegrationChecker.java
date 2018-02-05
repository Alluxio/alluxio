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

import alluxio.PropertyKey;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Serializable;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The Spark integration checker includes a spark job to test the integration of Spark with Alluxio.
 * This class will be triggered in client and cluster mode through spark-submit.
 *
 * This checker requires a running Spark cluster, but does not require a running Alluxio cluster.
 * It will check whether Alluxio classes and Alluxio filesystem can be recognized in Spark
 * driver and executors.
 */
public class SparkIntegrationChecker implements Serializable{
  private static final long serialVersionUID = 1106074873546987859L;

  @Parameter(names = {"--partitions"}, description = "The user defined partition number, "
      + "by default the value is 10")
  private int mPartitions;

  private static final Logger LOG = LoggerFactory.getLogger(SparkIntegrationChecker.class);
  private List<Tuple2<Status, String>> mSparkJobResult = null;
  private boolean mAlluxioHAMode = false;
  private String mZookeeperAddress = "";

  /** The Spark driver and executors performIntegrationChecks results. */
  private enum Status {
    FAIL_TO_FIND_CLASS, // Spark driver or executors cannot recognize Alluxio classes
    FAIL_TO_FIND_FS,    // Spark driver or executors cannot recognize Alluxio filesystem
    FAIL_TO_SUPPORT_HA,    // Spark driver cannot support Alluxio-HA mode
    SUCCESS;
  }

  /**
   * Implements Spark with Alluxio integration checker.
   *
   * @param sc current JavaSparkContext
   * @param printWriter save user-facing messages to a generated file
   * @return performIntegrationChecks results
   */
  private Status run(JavaSparkContext sc, PrintWriter printWriter) {
    // Checks whether Spark driver can recognize Alluxio classes and filesystem
    Status driverStatus = performIntegrationChecks();
    String driverAddress = sc.getConf().get("spark.driver.host");
    switch (driverStatus) {
      case FAIL_TO_FIND_CLASS:
        printWriter.printf("Spark driver: %s failed to recognize Alluxio classes.%n%n",
            driverAddress);
        return driverStatus;
      case FAIL_TO_FIND_FS:
        printWriter.printf("Spark driver: %s failed to recognize Alluxio filesystem.%n%n",
            driverAddress);
        return driverStatus;
      default:
        printWriter.printf("Spark driver: %s can recognize Alluxio filesystem.%n%n",
            driverAddress);
        break;
    }

    // Supports Alluxio high availability mode
    if (alluxio.Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
      mAlluxioHAMode = true;
      if (!alluxio.Configuration.containsKey(PropertyKey.ZOOKEEPER_ADDRESS)) {
        printWriter.println("Alluxio is in high availability mode"
            + ", but Zookeeper address has not been set.\n");
        return Status.FAIL_TO_SUPPORT_HA;
      } else {
        mZookeeperAddress = alluxio.Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS);
      }
    }

    return runSparkJob(sc);
  }

  /**
   * Spark job to check whether Spark executors can recognize Alluxio filesystem.
   *
   * @param sc current JavaSparkContext
   * @return Spark job result
   */
  private Status runSparkJob(JavaSparkContext sc) {
    // Generates a list of integer for testing
    List<Integer> nums = IntStream.rangeClosed(1, mPartitions).boxed().collect(Collectors.toList());

    JavaRDD<Integer> dataSet = sc.parallelize(nums, mPartitions);

    // Runs a Spark job to check whether Spark executors can recognize Alluxio
    JavaPairRDD<Status, String> extractedStatus = dataSet
        .mapToPair(s -> new Tuple2<>(performIntegrationChecks(), getAddress()));

    // Merges the IP addresses that can/cannot recognize Alluxio
    JavaPairRDD<Status, String> mergeStatus = extractedStatus.reduceByKey((a, b)
        -> a.contains(b) ?  a : (b.contains(a) ? b : a + " " + b), 1);

    mSparkJobResult = mergeStatus.collect();

    // If one executor cannot recognize Alluxio, something wrong has happened
    boolean canRecognizeClass = true;
    boolean canRecognizeFS = true;
    for (Tuple2<Status, String> op : mSparkJobResult) {
      if (op._1().equals(Status.FAIL_TO_FIND_CLASS)) {
        canRecognizeClass = false;
      }
      if (op._1().equals(Status.FAIL_TO_FIND_FS)) {
        canRecognizeFS = false;
      }
    }

    return canRecognizeClass ? (canRecognizeFS ? Status.SUCCESS
        : Status.FAIL_TO_FIND_FS) : Status.FAIL_TO_FIND_CLASS;
  }

  /**
   * @return if this Spark driver or executors can recognize Alluxio classes and filesystem
   */
  private Status performIntegrationChecks() {
    // Checks if Spark driver or executors can recognize Alluxio classes
    try {
      // Checks if Spark driver or executors can recognize Alluxio common classes
      Class.forName("alluxio.AlluxioURI");
      // Checks if Spark driver or executors can recognize Alluxio core client classes
      Class.forName("alluxio.client.file.BaseFileSystem");
      Class.forName("alluxio.hadoop.AlluxioFileSystem");
    } catch (ClassNotFoundException e) {
      LOG.error("Failed to find Alluxio classes on classpath ", e);
      return Status.FAIL_TO_FIND_CLASS;
    }

    // Checks if Spark driver or executors can recognize Alluxio filesystem
    try {
      FileSystem.getFileSystemClass("alluxio", new Configuration());
    } catch (Exception e) {
      LOG.error("Failed to find Alluxio filesystem ", e);
      return Status.FAIL_TO_FIND_FS;
    }

    return Status.SUCCESS;
  }

  /**
   * @return the current Spark executor IP address
   */
  private String getAddress() {
    String address;
    try {
      address = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      LOG.debug("cannot get host address");
      address = "unknown address";
    }
    return address;
  }

  /**
   * Saves related Spark and Alluxio configuration information.
   *
   * @param conf the current SparkConf
   * @param printWriter save user-facing messages to a generated file
   */
  private void printConfigInfo(SparkConf conf, PrintWriter printWriter) {
    // Prints the current time to separate integration checker results
    SimpleDateFormat df = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
    Date date = new Date();
    printWriter.printf("%n%n%n***** The integration checker ran at %s. *****%n%n",
        df.format(date));

    // Gets Spark configurations
    if (conf.contains("spark.master")) {
      printWriter.printf("Spark master is: %s.%n%n", conf.get("spark.master"));
    }
    if (conf.contains("spark.submit.deployMode")) {
      printWriter.printf("spark-submit deploy mode is: %s.%n%n",
          conf.get("spark.submit.deployMode"));
    }
    if (conf.contains("spark.driver.extraClassPath")) {
      printWriter.printf("spark.driver.extraClassPath includes jar paths: %s.%n%n",
          conf.get("spark.driver.extraClassPath"));
    }
    if (conf.contains("spark.executor.extraClassPath")) {
      printWriter.printf("spark.executor.extraClassPath includes jar paths: %s.%n%n",
          conf.get("spark.executor.extraClassPath"));
    }
  }

  /**
   * Saves Spark with Alluixo integration checker results.
   *
   * @param resultStatus Spark job result
   * @param printWriter save user-facing messages to a generated file
   */
  private void printResultInfo(Status resultStatus, PrintWriter printWriter) {
    if (mAlluxioHAMode) {
      printWriter.println("Alluixo is running in high availbility mode.");
      if (!mZookeeperAddress.isEmpty()) {
        printWriter.printf("alluxio.zookeeper.address is %s.%n%n", mZookeeperAddress);
      }
    }

    if (mSparkJobResult != null) {
      for (Tuple2<Status, String> sjr : mSparkJobResult) {
        switch (sjr._1()) {
          case FAIL_TO_FIND_CLASS:
            printWriter.printf("Spark executors of IP addresses: %s "
                + "cannot recognize Alluxio classes.%n%n", sjr._2);
            break;
          case FAIL_TO_FIND_FS:
            printWriter.printf("Spark executors of IP addresses %s "
                + "cannot recognize Alluxio filesystem.%n%n", sjr._2);
            break;
          default:
            printWriter.printf("Spark executors of IP addresses %s "
                + "can recognize Alluxio filesystem.%n%n", sjr._2);
            break;
        }
      }
    }

    switch (resultStatus) {
      case FAIL_TO_FIND_CLASS:
        printWriter.println("Please check the spark.driver.extraClassPath "
            + "and spark.executor.extraClassPath properties in "
            + "${SPARK_HOME}/conf/spark-defaults.conf\n");
        printWriter.println("If Alluxio client jar path has been set correctly, "
            + "please check whether the Alluxio client jar has been distributed "
            + "on the classpath of all Spark cluster nodes.");
        printWriter.println("For details, please refer to: "
            + "https://www.alluxio.org/docs/master/en/Running-Spark-on-Alluxio.html\n");
        printWriter.println("***** Integration test failed. *****");
        break;
      case FAIL_TO_FIND_FS:
        printWriter.println("Please check the fs.alluxio.impl property "
            + "in ${SPARK_HOME}/conf/core-site.xml\n");
        printWriter.println("For details, please refer to: "
            + "https://www.alluxio.org/docs/master/en/Debugging-Guide.html"
            + "#q-why-do-i-see-exceptions-like-no-filesystem-for-scheme-alluxio\n");
        printWriter.println("***** Integration test failed. *****");
        break;
      case FAIL_TO_SUPPORT_HA:
        printWriter.println("Please check the alluxio.zookeeper.address property "
            + "in ${SPARK_HOME}/conf/core-site.xml\n");
        printWriter.println("For details, please refer to: "
            + "https://www.alluxio.org/docs/master/en/Running-Spark-on-Alluxio.html\n");
        printWriter.println("***** Integration test failed. *****");
        break;
      default:
        printWriter.println("***** Integration test passed. *****");
        break;
    }
  }

  /**
   * Main function will be triggered via spark-submit.
   *
   * @param args optional argument mPartitions may be passed in
   */
  public static void main(String[] args) throws Exception {
    SparkIntegrationChecker checker = new SparkIntegrationChecker();
    JCommander jCommander = new JCommander(checker, args);
    jCommander.setProgramName("SparkIntegrationChecker");

    // Creates a file to save user-facing messages
    FileWriter fw = new FileWriter("./SparkOutputFile.txt", true);
    BufferedWriter bw = new BufferedWriter(fw);
    PrintWriter printWriter = new PrintWriter(bw);

    // Starts the Java Spark Context
    SparkConf conf = new SparkConf().setAppName(SparkIntegrationChecker.class.getName());
    JavaSparkContext sc = new JavaSparkContext(conf);

    checker.printConfigInfo(conf, printWriter);
    Status resultStatus = checker.run(sc, printWriter);
    checker.printResultInfo(resultStatus, printWriter);

    printWriter.flush();
    printWriter.close();

    System.exit(resultStatus.equals(Status.SUCCESS) ? 0 : 1);
  }
}
