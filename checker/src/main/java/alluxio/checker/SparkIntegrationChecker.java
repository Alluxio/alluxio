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

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

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
 * It will check whether Alluxio class and Alluxio filesystem can be recognize in Spark
 * driver and executors.
 */
public class SparkIntegrationChecker {
  private static final Logger LOG = LoggerFactory.getLogger(SparkIntegrationChecker.class);
  private static List<Tuple2<Integer, String>> sSparkJobResult;
  private static boolean sAlluxioHAMode = false;
  private static String sZookeeperAddress = "";

  /**
   * Implements check integration of Spark with Alluxio.
   *
   * @param sc current JavaSparkContext
   * @return 0 succeed, 1 fail to recognize Alluxio classes,
   * 2 fail to recognize Alluxio filesystem, 3 fail to support Alluxio HA mode
   */
  private static int checkIntegration(JavaSparkContext sc) {
    // check if Spark driver can recognize Alluxio classes
    try {
      Class alluxioURIClass = Class.forName("alluxio.AlluxioURI");
      LOG.info("Spark driver can find: " + alluxioURIClass.getName());
    } catch (ClassNotFoundException e) {
      LOG.error("Spark driver cannot find AlluxioURI class:" + e);
      return 1;
    }

    try {
      Class alluxioBaseFSClass = Class.forName("alluxio.client.file.BaseFileSystem");
      LOG.info("Spark driver can find: " + alluxioBaseFSClass.getName());
    } catch (ClassNotFoundException e) {
      LOG.error("Spark driver cannot find alluxio.client.file.BaseFileSystem class:" + e);
      return 1;
    }

    try {
      Class alluxioHadoopFSClass = Class.forName("alluxio.hadoop.AlluxioFileSystem");
      LOG.info("Spark driver can find: " + alluxioHadoopFSClass.getName());
    } catch (ClassNotFoundException e) {
      LOG.error("Spark driver cannot find alluxio.hadoop.AlluxioFileSystem class:" + e);
      return 1;
    }

    // check if Spark driver can recognize Alluxio filesystem
    try {
      Class alluxioFSClass = FileSystem.getFileSystemClass("alluxio", new Configuration());
      LOG.info("Spark driver can recognize Alluxio filesystem: " + alluxioFSClass.getName());
    } catch (Exception e) {
      LOG.error("Spark driver cannot recognize Alluxio filesystem: " + e);
      return 2;
    }

    // support Alluxio high availability mode
    if (alluxio.Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
      sAlluxioHAMode = true;
      try {
        sZookeeperAddress = alluxio.Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS);
      } catch (RuntimeException e) {
        LOG.error("Zookeeper address has not been set:" + e);
        return 3;
      }
    }

    // run a Spark job to check Spark executors configuration
    return runOperations(sc);
  }

  /**
   * Spark job to check whether Spark executors can recognize Alluxio filesystem.
   *
   * @param sc current JavaSparkContext
   * @return 0 on success, 1 on failures
   */
  private static int runOperations(JavaSparkContext sc) {
    // generate a list of integer for testing
    List<Integer> nums = IntStream.rangeClosed(1, 1000).boxed().collect(Collectors.toList());

    //TODO(lu) set a reasonable parition number to check as many executors as possible
    JavaRDD<Integer> dataSet = sc.parallelize(nums);

    // run a Spark job to check whether Spark executors can recognize Alluxio
    JavaPairRDD<Integer, String> extractedStatus = dataSet
        .mapToPair(s -> new Tuple2<>(getStatus(s), getAddress(s)));

    // merge the IP addresses that can/cannot recognize Alluxio
    JavaPairRDD<Integer, String> mergeStatus = extractedStatus.reduceByKey((a, b) -> merge(a, b));

    sSparkJobResult = mergeStatus.collect();

    // If one executor cannot recognize Alluxio, something wrong happen
    boolean canRecognizeClass = true;
    boolean canRecognizeFS = true;
    for (Tuple2<Integer, String> op : sSparkJobResult) {
      if (op._1() == 1) {
        canRecognizeClass = false;
      }
      if (op._1() == 2) {
        canRecognizeFS = false;
      }
    }
    return canRecognizeClass ? (canRecognizeFS ? 0 : 2) : 1;
  }

  /**
   * Check if this Spark executor can recognize Alluxio filesystem.
   *
   * @param s the integer pass in
   * @return 0 succeed, 1 fail to recognize Alluxio classes,
   * 2 fail to recognize Alluxio filesystem
   */
  private static int getStatus(Integer s) {
    // check if Spark executors can recognize Alluxio classes
    try {
      Class alluxioURIClass = Class.forName("alluxio.AlluxioURI");
      LOG.info("Spark driver can find: " + alluxioURIClass.getName());
    } catch (ClassNotFoundException e) {
      LOG.error("Spark driver cannot find AlluxioURI class:" + e);
      return 1;
    }

    try {
      Class alluxioBaseFSClass = Class.forName("alluxio.client.file.BaseFileSystem");
      LOG.info("Spark driver can find: " + alluxioBaseFSClass.getName());
    } catch (ClassNotFoundException e) {
      LOG.error("Spark driver cannot find alluxio.client.file.BaseFileSystem class:" + e);
      return 1;
    }

    try {
      Class alluxioHadoopFSClass = Class.forName("alluxio.hadoop.AlluxioFileSystem");
      LOG.info("Spark driver can find: " + alluxioHadoopFSClass.getName());
    } catch (ClassNotFoundException e) {
      LOG.error("Spark driver cannot find alluxio.hadoop.AlluxioFileSystem class:" + e);
      return 1;
    }

    // check if Spark executors can recognize Alluxio filesystem
    try {
      Class alluxioFSClass = FileSystem.getFileSystemClass("alluxio", new Configuration());
      LOG.info("Spark driver can recognize Alluxio filesystem: " + alluxioFSClass.getName());
    } catch (Exception e) {
      LOG.error("Spark driver cannot recognize Alluxio filesystem: " + e);
      return 2;
    }

    return 0;
  }

  /**
   * Get the current Spark executor IP address.
   *
   * @param s the integer pass in
   * @return the current Spark executor IP address
   */
  private static String getAddress(Integer s) {
    String address;
    try {
      address = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      LOG.info("cannot get host address");
      address = "unknown address";
    }
    return address;
  }

  /**
   * Merge the IP addresses that have the same key.
   *
   * @param a, b two IP addresses that both can recognize Alluxio or both cannot
   * @return merged Spark executor IP addresses
   */
  private static String merge(String a, String b) {
    if (a.contains(b)) {
      return a;
    } else if (b.contains(a)) {
      return b;
    } else {
      return a + "; " + b;
    }
  }

  /**
   * Print the user-facing messages.
   *
   * @param resultCode the result code get from checkIntegration
   * @param conf the current SparkConf
   */
  private static void printMessage(int resultCode, SparkConf conf) {
    if (sAlluxioHAMode) {
      System.out.println("Alluixo is running in high availbility mode.");
    }

    if (resultCode != 3) {
      for (Tuple2<Integer, String> sjr : sSparkJobResult) {
        if (sjr._1() == 1) {
          System.out.println("IP addresses: " + sjr._2 + " cannot recognize Alluxio classes.");
        } else if (sjr._1() == 2) {
          System.out.println("IP addresses: " + sjr._2 + " cannot recognize Alluxio filesystem.");
        } else {
          System.out.println("IP addresses: " + sjr._2 + " can recognize Alluxio filesystem.");
        }
      }
    }

    try {
      System.out.println("Alluxio jar path set in spark.driver.extraClassPath is "
          + conf.get("spark.driver.extraClassPath") + ".");
      System.out.println("Alluxio jar path set in spark.executor.extraClassPath is "
          + conf.get("spark.executor.extraClassPath") + ".");
    } catch (NoSuchElementException e) {
      //
    }

    if (sAlluxioHAMode && !sZookeeperAddress.equals("")) {
      System.out.println("alluxio.zookeeper.address is " + sZookeeperAddress + ".");
    }
  }

  /**
   * Print Spark and Alluxio integration helping information.
   *
   * @param args no argument needed in this class
   */
  public static void main(String[] args) {
    // start the Java Spark Context
    SparkConf conf = new SparkConf().setAppName(SparkIntegrationChecker.class.getName());
    JavaSparkContext sc = new JavaSparkContext(conf);

    int resultCode = checkIntegration(sc);
    switch (resultCode) {
      case 1:
        System.out.println("Please check the spark.driver.extraClassPath "
            + "and spark.executor.extraClassPath.");
        System.out.println("Integration test failed.");
        break;
      case 2:
        System.out.println("Please check the fs.alluxio.impl property.");
        System.out.println("For details, please refer to: "
            + "https://www.alluxio.org/docs/master/en/Debugging-Guide.html"
            + "#q-why-do-i-see-exceptions-like-no-filesystem-for-scheme-alluxio");
        System.out.println("Integration test failed.");
        break;
      case 3:
        System.out.println("Please check the alluxio.zookeeper.address property.");
        System.out.println("Integration test failed.");
        break;
      default:
        System.out.println("Integration test passed.");
        break;
    }
    printMessage(resultCode, conf);
    System.exit(resultCode);
  }
}
