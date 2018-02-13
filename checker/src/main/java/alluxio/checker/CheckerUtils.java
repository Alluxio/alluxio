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
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

/**
 * Class for common methods used by integration checkers.
 */
public final class CheckerUtils {
  private static final Logger LOG = LoggerFactory.getLogger(CheckerUtils.class);

  private CheckerUtils() {} // prevent instantiation

  /** The performIntegrationChecks results. */
  public enum Status {
    FAIL_TO_FIND_CLASS, // Current node cannot recognize Alluxio classes
    FAIL_TO_FIND_FS, // Current node cannot recognize Alluxio filesystem
    FAIL_TO_SUPPORT_HA, // Alluxio HA configuration is invalid
    SUCCESS;
  }

  /**
   * @return if the current node can recognize Alluxio classes and filesystem
   */
  public static Status performIntegrationChecks() {
    // Checks if the current node can recognize Alluxio classes
    try {
      // Checks if the current node can recognize Alluxio common classes
      Class.forName("alluxio.AlluxioURI");
      // Checks if current node can recognize Alluxio core client classes
      Class.forName("alluxio.client.file.BaseFileSystem");
      Class.forName("alluxio.hadoop.AlluxioFileSystem");
    } catch (ClassNotFoundException e) {
      LOG.error("Failed to find Alluxio classes on classpath", e);
      return Status.FAIL_TO_FIND_CLASS;
    }

    // Checks if the current node can recognize Alluxio filesystem
    try {
      FileSystem.getFileSystemClass("alluxio", new Configuration());
    } catch (Exception e) {
      LOG.error("Failed to find Alluxio filesystem", e);
      return Status.FAIL_TO_FIND_FS;
    }

    return Status.SUCCESS;
  }

  /**
   * @return the current node IP address
   */
  public static String getLocalAddress() {
    String address;
    try {
      address = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      LOG.warn("Cannot get local address of current node.");
      address = "unknown address";
    }
    return address;
  }

  /**
   * Collects and saves the Status of checked nodes.
   *
   * @param map the transformed integration checker results
   * @param reportWriter save the result to corresponding checker report
   * @return the result information of the checked nodes
   */
  public static Status printNodesResults(Map<Status, List<String>> map, PrintWriter reportWriter) {
    boolean canFindClass = true;
    boolean canFindFS = true;

    for (Map.Entry<Status, List<String>> entry : map.entrySet()) {
      String nodeAddresses = String.join(" ", entry.getValue());
      switch (entry.getKey()) {
        case FAIL_TO_FIND_CLASS:
          canFindClass = false;
          reportWriter.printf("Nodes of IP addresses: %s "
              + "cannot recognize Alluxio classes.%n%n", nodeAddresses);
          break;
        case FAIL_TO_FIND_FS:
          canFindFS = false;
          reportWriter.printf("Nodes of IP addresses: %s "
              + "cannot recognize Alluxio filesystem.%n%n", nodeAddresses);
          break;
        default:
          reportWriter.printf("Nodes of IP addresses: %s "
              + "can recognize Alluxio filesystem.%n%n", nodeAddresses);
      }
    }
    return canFindClass ? (canFindFS ? Status.SUCCESS : Status.FAIL_TO_FIND_FS)
        : Status.FAIL_TO_FIND_CLASS;
  }
}
