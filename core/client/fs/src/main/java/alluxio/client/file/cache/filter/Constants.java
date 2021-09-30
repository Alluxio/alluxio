/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file.cache.filter;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public final class Constants {
  public static final int K = 1000;
  public static final int M = K * 1000;

  public static final int KB = 1024;
  public static final int MB = KB * 1024;
  public static final int GB = MB * 1024;
  public static final long TB = GB * 1024L;
  public static final long PB = TB * 1024L;

  public static final long SECOND = 1000;
  public static final long MINUTE = SECOND * 60L;
  public static final long HOUR = MINUTE * 60L;
  public static final long DAY = HOUR * 24L;

  // Cuckoo constants
  public static final int MAX_BFS_PATH_LEN = 5;
}
