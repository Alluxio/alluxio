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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A spark job to test the integration of Spark with Alluxio. This class will be triggered
 * in yarn-client and yarn-cluster mode through spark-submit.
 *
 * This job requires a running Spark cluster, but not requires a running Alluxio cluster.
 * It will check whether Alluxio classes and Alluxio filesystem can be recognized in Spark
 * driver and executors.
 */
public class SparkIntegrationChecker implements Serializable{

  private static final long serialVersionUID = 1L;

  @Parameter(names = {"-p", "--partition"}, description = "The user defined partition number, "
      + "by default the value is 10")
  private int mPartitions;

  private static final Logger LOG = LoggerFactory.getLogger(SparkIntegrationChecker.class);
  private List<Tuple2<Status, String>> mSparkJobResult;
  private boolean mAlluxioHAMode = false;
  private String mZookeeperAddress = "";

  /* The Spark driver and executors performIntegrationChecks results*/
  private enum Status {
    FAILCLASS, // Spark driver or executors cannot recognize Alluxio classes
    FAILHA,    // Spark driver cannot support Alluxio-HA mode
    FAILFS,    // Spark driver or executors cannot recognize Alluxio filesystem
    SUCCESS;
  }

  /**
   * Implements check integration of Spark with Alluxio.
   *
   * @param sc current JavaSparkContext
   * @return Status performIntegrationChecks results
   */
  private Status run(JavaSparkContext sc) {
    // Checks whether Spark driver can recognize Alluxio classes and filesystem
    Status driverStatus = performIntegrationChecks();
    if (driverStatus.equals(Status.FAILCLASS)) {
      LOG.error("Spark driver: " + getAddress() + " failed to recognize Alluxio classes.");
      return driverStatus;
    } else if (driverStatus.equals(Status.FAILFS)) {
      LOG.error("Spark driver: " + getAddress() + " failed to recognize Alluxio filesystem.");
      return driverStatus;
    } else {
      LOG.info("Spark driver: " + getAddress() + " can recognize Alluxio filesystem.");
    }

    // Supports Alluxio high availability mode
    if (alluxio.Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
      mAlluxioHAMode = true;
      if (!alluxio.Configuration.containsKey(PropertyKey.ZOOKEEPER_ADDRESS)) {
        LOG.error("Zookeeper address has not been set:");
        return Status.FAILHA;
      }
    }

    return runSparkJob(sc);
  }

  /**
   * Spark job to check whether Spark executors can recognize Alluxio filesystem.
   *
   * @param sc current JavaSparkContext
   * @return Status
   */
  private Status runSparkJob(JavaSparkContext sc) {
    // Generates a list of integer for testing
    List<Integer> nums = IntStream.rangeClosed(1, mPartitions).boxed().collect(Collectors.toList());

    JavaRDD<Integer> dataSet = sc.parallelize(nums, mPartitions);

    // Runs a Spark job to check whether Spark executors can recognize Alluxio
    JavaPairRDD<Status, String> extractedStatus = dataSet
        .mapToPair(s -> new Tuple2<>(performIntegrationChecks(), getAddress()));

    // Merges the IP addresses that can/cannot recognize Alluxio
    JavaPairRDD<Status, String> mergeStatus = extractedStatus.reduceByKey((a, b) -> merge(a, b));

    mSparkJobResult = mergeStatus.collect();

    // If one executor cannot recognize Alluxio, something wrong has happened
    boolean canRecognizeClass = true;
    boolean canRecognizeFS = true;
    for (Tuple2<Status, String> op : mSparkJobResult) {
      if (op._1().equals(Status.FAILCLASS)) {
        canRecognizeClass = false;
      }
      if (op._1().equals(Status.FAILFS)) {
        canRecognizeFS = false;
      }
    }
    return canRecognizeClass ? (canRecognizeFS ? Status.SUCCESS : Status.FAILFS) : Status.FAILCLASS;
  }

  /**
   * Checks if this Spark driver or executors can recognize Alluxio classes and filesystem.
   *
   * @return Status
   */
  private Status performIntegrationChecks() {
    // Checks if Spark driver or executors can recognize Alluxio classes
    try {
      Class.forName("alluxio.AlluxioURI");
      Class.forName("alluxio.client.file.BaseFileSystem");
      Class.forName("alluxio.hadoop.AlluxioFileSystem");
    } catch (ClassNotFoundException e) {
      LOG.error("Failed to find Alluxio classes on classpath ", e);
      return Status.FAILCLASS;
    }

    // Checks if Spark driver or executors can recognize Alluxio filesystem
    try {
      FileSystem.getFileSystemClass("alluxio", new Configuration());
    } catch (Exception e) {
      LOG.error("Failed to find Alluxio filesystem ", e);
      return Status.FAILFS;
    }

    return Status.SUCCESS;
  }

  /**
   * Gets the current Spark driver or executor IP address.
   *
   * @return the current Spark driver or executor IP address
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
   * Merges the IP addresses that have the same Status.
   *
   * @param addressA,addressB two IP addresses that have the same Status
   * @return merged Spark executor IP addresses
   */
  private String merge(String addressA, String addressB) {
    if (addressA.contains(addressB)) {
      return addressA;
    } else if (addressB.contains(addressA)) {
      return addressB;
    } else {
      return addressA + "; " + addressB;
    }
  }

  /**
   * Prints the user-facing messages.
   *
   * @param resultStatus the result code get from checkIntegration
   * @param conf the current SparkConf
   */
  private void printMessage(Status resultStatus, SparkConf conf) {
    if (mAlluxioHAMode) {
      LOG.info("Alluixo is running in high availbility mode.");
    }

    try {
      for (Tuple2<Status, String> sjr : mSparkJobResult) {
        if (sjr._1().equals(Status.FAILCLASS)) {
          LOG.error("Spark executors of IP addresses: " + sjr._2
              + " cannot recognize Alluxio classes.");
        } else if (sjr._1().equals(Status.FAILFS)) {
          LOG.error("Spark executors of IP addresses: " + sjr._2
              + " cannot recognize Alluxio filesystem.");
        } else {
          LOG.info("Spark executors of IP addresses: " + sjr._2
              + " can recognize Alluxio filesystem.");
        }
      }
    } catch (NullPointerException e) {
      // If Spark driver integration checks failed, sSparkJobResult will not be assigned
    }

    try {
      LOG.info("spark.driver.extraClassPath includes jar paths: "
          + conf.get("spark.driver.extraClassPath") + ".");
      LOG.info("spark.executor.extraClassPath includes jar paths: "
          + conf.get("spark.executor.extraClassPath") + ".");
    } catch (NoSuchElementException e) {
      // Users may have not set Alluxio jar paths
    }

    if (mAlluxioHAMode && mZookeeperAddress.isEmpty()) {
      LOG.info("alluxio.zookeeper.address is " + mZookeeperAddress + ".");
    }

    switch (resultStatus) {
      case FAILCLASS:
        LOG.error("Please check the spark.driver.extraClassPath "
            + "and spark.executor.extraClassPath.");
        System.out.println("Integration test failed.");
        break;
      case FAILFS:
        LOG.error("Please check the fs.alluxio.impl property.");
        LOG.error("For details, please refer to: "
            + "https://www.alluxio.org/docs/master/en/Debugging-Guide.html"
            + "#q-why-do-i-see-exceptions-like-no-filesystem-for-scheme-alluxio");
        System.out.println("Integration test failed.");
        break;
      case FAILHA:
        LOG.error("Please check the alluxio.zookeeper.address property.");
        System.out.println("Integration test failed.");
        break;
      default:
        System.out.println("Integration test passed.");
        break;
    }
  }

  /**
   * Main function will be triggered via spark-submit.
   *
   * @param args optional argument partitions may be passed in
   */
  public static void main(String[] args) {
    SparkIntegrationChecker checker = new SparkIntegrationChecker();
    JCommander jCommander = new JCommander(checker, args);
    jCommander.setProgramName("SparkIntegrationChecker");

    // Starts the Java Spark Context
    SparkConf conf = new SparkConf().setAppName(SparkIntegrationChecker.class.getName());
    JavaSparkContext sc = new JavaSparkContext(conf);
    sc.setLogLevel("INFO");

    Status resultStatus = checker.run(sc);
    checker.printMessage(resultStatus, conf);
    System.exit(resultStatus.equals(Status.SUCCESS) ? 0 : 1);
  }
}
