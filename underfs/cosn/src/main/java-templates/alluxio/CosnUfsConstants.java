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

/**
 * Ufs constants from compilation time by maven.
 */
public final class CosnUfsConstants {
  /* COSN version, specified in maven property. **/
  public static final String UFS_COSN_VERSION = "${ufs.cosn.version}";

  private CosnUfsConstants() {} // prevent instantiation
}
