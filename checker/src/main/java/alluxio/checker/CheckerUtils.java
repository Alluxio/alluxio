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

import java.net.InetAddress;
import java.net.UnknownHostException;

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
}
