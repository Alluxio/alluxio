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
import alluxio.checker.CommonCheckerUtils.Status;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The Spark integration checker includes a Spark job to test the integration of Spark with Alluxio.
 * This class will be triggered in client and cluster mode through spark-submit.
 *
 * This checker requires a running Spark cluster, but does not require a running Alluxio cluster.
 * It will check whether Alluxio classes and Alluxio filesystem can be recognized in Spark
 * driver and executors.
 */
public class SparkIntegrationChecker implements Serializable{
  private static final Logger LOG = LoggerFactory.getLogger(SparkIntegrationChecker.class);
  private static final long serialVersionUID = 1106074873546987859L;
  private static final String FAIL_TO_FIND_CLASS_MESSAGE = "Please check the "
      + "spark.driver.extraClassPath and spark.executor.extraClassPath properties in "
      + "${SPARK_HOME}/conf/spark-defaults.conf.\n\n"
      + "If Alluxio client jar path has been set correctly, please check whether the "
      + "Alluxio client jar has been distributed on the classpath of all Spark cluster nodes.\n\n"
      + "For details, please refer to: "
      + "https://www.alluxio.org/docs/master/en/Running-Spark-on-Alluxio.html\n";
  private static final String FAIL_TO_FIND_FS_MESSAGE = "Please check the "
      + "fs.alluxio.impl property in ${SPARK_HOME}/conf/core-site.xml.\n\n"
      + "For details, please refer to: "
      + "https://www.alluxio.org/docs/master/en/Debugging-Guide.html"
      + "#q-why-do-i-see-exceptions-like-no-filesystem-for-scheme-alluxio\n";
  private static final String FAIL_TO_SUPPORT_HA_MESSAGE = "Please check the "
      + "alluxio.zookeeper.address property in ${SPARK_HOME}/conf/core-site.xml\n\n"
      + "For details, please refer to: "
      + "https://www.alluxio.org/docs/master/en/Running-Spark-on-Alluxio.html\n";
  private static final String TEST_FAILED_MESSAGE = "***** Integration test failed. *****";
  private static final String TEST_PASSED_MESSAGE = "***** Integration test passed. *****";

  @Parameter(names = {"--partitions"}, description = "The user defined partition number")
  private int mPartitions = 10;

  private List<Tuple2<Status, String>> mSparkJobResult = null;
  private boolean mAlluxioHAMode = false;
  private String mZookeeperAddress = "";

  /**
   * Implements Spark with Alluxio integration checker.
   *
   * @param sc current JavaSparkContext
   * @param reportWriter save user-facing messages to a generated file
   * @return performIntegrationChecks results
   */
  private Status run(JavaSparkContext sc, PrintWriter reportWriter) {
    // Checks whether Spark driver can recognize Alluxio classes and filesystem
    Status driverStatus = CommonCheckerUtils.performIntegrationChecks();
    String driverAddress = sc.getConf().get("spark.driver.host");
    switch (driverStatus) {
      case FAIL_TO_FIND_CLASS:
        reportWriter.printf("Spark driver: %s failed to recognize Alluxio classes.%n%n",
            driverAddress);
        return driverStatus;
      case FAIL_TO_FIND_FS:
        reportWriter.printf("Spark driver: %s failed to recognize Alluxio filesystem.%n%n",
            driverAddress);
        return driverStatus;
      default:
        reportWriter.printf("Spark driver: %s can recognize Alluxio filesystem.%n%n",
            driverAddress);
        break;
    }

    // Supports Alluxio high availability mode
    if (alluxio.Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
      mAlluxioHAMode = true;
      if (!alluxio.Configuration.containsKey(PropertyKey.ZOOKEEPER_ADDRESS)) {
        reportWriter.println("Alluxio is in high availability mode, "
            + "but Zookeeper address has not been set.\n");
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
        .mapToPair(s -> new Tuple2<>(CommonCheckerUtils.performIntegrationChecks(),
            CommonCheckerUtils.getAddress()));

    // Merges the IP addresses that can/cannot recognize Alluxio
    JavaPairRDD<Status, String> mergeStatus = extractedStatus.reduceByKey((a, b)
        -> a.contains(b) ?  a : (b.contains(a) ? b : a + " " + b),
        (mPartitions < 10 ? 1 : mPartitions / 10));

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
   * Saves related Spark and Alluxio configuration information.
   *
   * @param conf the current SparkConf
   * @param reportWriter save user-facing messages to a generated file
   */
  private void printConfigInfo(SparkConf conf, PrintWriter reportWriter) {
    // Prints the current time to separate integration checker results
    SimpleDateFormat df = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
    Date date = new Date();
    reportWriter.printf("%n%n%n***** The integration checker ran at %s. *****%n%n",
        df.format(date));

    // Gets Spark configurations
    if (conf.contains("spark.master")) {
      reportWriter.printf("Spark master is: %s.%n%n", conf.get("spark.master"));
    }
    if (conf.contains("spark.submit.deployMode")) {
      reportWriter.printf("spark-submit deploy mode is: %s.%n%n",
          conf.get("spark.submit.deployMode"));
    }
    if (conf.contains("spark.driver.extraClassPath")) {
      reportWriter.printf("spark.driver.extraClassPath includes jar paths: %s.%n%n",
          conf.get("spark.driver.extraClassPath"));
    }
    if (conf.contains("spark.executor.extraClassPath")) {
      reportWriter.printf("spark.executor.extraClassPath includes jar paths: %s.%n%n",
          conf.get("spark.executor.extraClassPath"));
    }
  }

  /**
   * Saves Spark with Alluixo integration checker results.
   *
   * @param resultStatus Spark job result
   * @param reportWriter save user-facing messages to a generated file
   */
  private void printResultInfo(Status resultStatus, PrintWriter reportWriter) {
    if (mAlluxioHAMode) {
      reportWriter.println("Alluixo is running in high availbility mode.");
      if (!mZookeeperAddress.isEmpty()) {
        reportWriter.printf("alluxio.zookeeper.address is %s.%n%n", mZookeeperAddress);
      }
    }

    if (mSparkJobResult != null) {
      for (Tuple2<Status, String> sjr : mSparkJobResult) {
        switch (sjr._1()) {
          case FAIL_TO_FIND_CLASS:
            reportWriter.printf("Spark executors of IP addresses: %s "
                + "cannot recognize Alluxio classes.%n%n", sjr._2);
            break;
          case FAIL_TO_FIND_FS:
            reportWriter.printf("Spark executors of IP addresses %s "
                + "cannot recognize Alluxio filesystem.%n%n", sjr._2);
            break;
          default:
            reportWriter.printf("Spark executors of IP addresses %s "
                + "can recognize Alluxio filesystem.%n%n", sjr._2);
            break;
        }
      }
    }

    switch (resultStatus) {
      case FAIL_TO_FIND_CLASS:
        reportWriter.println(FAIL_TO_FIND_CLASS_MESSAGE);
        reportWriter.println(TEST_FAILED_MESSAGE);
        break;
      case FAIL_TO_FIND_FS:
        reportWriter.println(FAIL_TO_FIND_FS_MESSAGE);
        reportWriter.println(TEST_FAILED_MESSAGE);
        break;
      case FAIL_TO_SUPPORT_HA:
        reportWriter.println(FAIL_TO_SUPPORT_HA_MESSAGE);
        reportWriter.println(TEST_FAILED_MESSAGE);
        break;
      default:
        reportWriter.println(TEST_PASSED_MESSAGE);
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
    FileWriter fileWriter = new FileWriter("./SparkIntegrationReport.txt", true);
    BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
    PrintWriter reportWriter = new PrintWriter(bufferedWriter);

    // Starts the Java Spark Context
    SparkConf conf = new SparkConf().setAppName(SparkIntegrationChecker.class.getName());
    JavaSparkContext sc = new JavaSparkContext(conf);

    checker.printConfigInfo(conf, reportWriter);
    Status resultStatus = checker.run(sc, reportWriter);
    checker.printResultInfo(resultStatus, reportWriter);

    reportWriter.flush();
    reportWriter.close();

    System.exit(resultStatus.equals(Status.SUCCESS) ? 0 : 1);
  }
}
