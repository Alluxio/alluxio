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

package alluxio.util.webui;

import alluxio.wire.WorkerInfo;

import java.util.Arrays;
import java.util.Collection;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The type Web utils.
 */
@ThreadSafe
public final class WebUtils {
  /**
   * Converts a byte array to string.
   *
   * @param data byte array
   * @param offset offset
   * @param length number of bytes to encode
   * @return string representation of the encoded byte sub-array
   */
  public static String convertByteArrayToStringWithoutEscape(byte[] data, int offset, int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = offset; i < length && i < data.length; i++) {
      sb.append((char) data[i]);
    }
    return sb.toString();
  }

  /**
   * Order the nodes by hostName and generate {@link NodeInfo} list for UI display.
   *
   * @param workerInfos the list of {@link WorkerInfo} objects
   * @return the list of {@link NodeInfo} objects
   */
  public static NodeInfo[] generateOrderedNodeInfos(Collection<WorkerInfo> workerInfos) {
    NodeInfo[] ret = new NodeInfo[workerInfos.size()];
    int index = 0;
    for (WorkerInfo workerInfo : workerInfos) {
      ret[index++] = new NodeInfo(workerInfo);
    }
    Arrays.sort(ret);

    return ret;
  }

  private WebUtils() {} // prevent instantiation
}
