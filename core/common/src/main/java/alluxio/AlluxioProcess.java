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

package alluxio;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility class to help distinguish between different types of Alluxio processes at runtime.
 */
@ThreadSafe
public final class AlluxioProcess {
  private static Type sType = Type.CLIENT;

  /**
   * Alluxio process types.
   */
  public enum Type {
    CLIENT,
    MASTER,
    WORKER
  }

  /**
   * @param type the {@link Type} to use
   */
  public static void setType(Type type) {
    sType = type;
  }

  /**
   * @return the {@link Type} of the current process
   */
  public static Type getType() {
    return sType;
  }

  private AlluxioProcess() {} // prevent instantiation
}
