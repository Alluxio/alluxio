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

package alluxio.underfs.hdfs;

import java.util.regex.Pattern;

import javax.annotation.Nullable;

/** The set of supported Hdfs versions. */
public enum HdfsVersion {
  HADOOP_1_0("hadoop-1.0", "(hadoop-?1\\.0(\\.(\\d+))?|1\\.0(\\.(\\d+)(-.*)?)?)"),
  HADOOP_1_2("hadoop-1.2", "(hadoop-?1\\.2(\\.(\\d+))?|1\\.2(\\.(\\d+)(-.*)?)?)"),
  HADOOP_2_2("hadoop-2.2", "(hadoop-?2\\.2(\\.(\\d+))?|2\\.2(\\.(\\d+)(-.*)?)?)"),
  HADOOP_2_3("hadoop-2.3", "(hadoop-?2\\.3(\\.(\\d+))?|2\\.3(\\.(\\d+)(-.*)?)?)"),
  HADOOP_2_4("hadoop-2.4", "(hadoop-?2\\.4(\\.(\\d+))?|2\\.4(\\.(\\d+)(-.*)?)?)"),
  HADOOP_2_5("hadoop-2.5", "(hadoop-?2\\.5(\\.(\\d+))?|2\\.5(\\.(\\d+)(-.*)?)?)"),
  HADOOP_2_6("hadoop-2.6", "(hadoop-?2\\.6(\\.(\\d+))?|2\\.6(\\.(\\d+)(-.*)?)?)"),
  HADOOP_2_7("hadoop-2.7", "(hadoop-?2\\.7(\\.(\\d+))?|2\\.7(\\.(\\d+)(-.*)?)?)"),
  HADOOP_2_8("hadoop-2.8", "(hadoop-?2\\.8(\\.(\\d+))?|2\\.8(\\.(\\d+)(-.*)?)?)"),
  HADOOP_2_9("hadoop-2.9", "(hadoop-?2\\.9(\\.(\\d+))?|2\\.9(\\.(\\d+)(-.*)?)?)"),
  HADOOP_3_0("hadoop-3.0", "(hadoop-?3\\.0(\\.(\\d+))?|3\\.0(\\.(\\d+)(-.*)?)?)"),
  HADOOP_3_1("hadoop-3.1", "(hadoop-?3\\.1(\\.(\\d+))?|3\\.1(\\.(\\d+)(-.*)?)?)"),
  HADOOP_3_2("hadoop-3.2", "(hadoop-?3\\.2(\\.(\\d+))?|3\\.2(\\.(\\d+)(-.*)?)?)"),
  HADOOP_3_3("hadoop-3.3", "(hadoop-?3\\.3(\\.(\\d+))?|3\\.3(\\.(\\d+)(-.*)?)?)"),
  ;

  private final String mCanonicalVersion;
  private final Pattern mVersionPattern;

  /**
   * Constructs an instance of {@link HdfsVersion}.
   *
   * @param canonicalVersion the canonical version of an HDFS
   * @param versionPattern the regex pattern of version for an HDFS
   */
  HdfsVersion(String canonicalVersion, String versionPattern) {
    mCanonicalVersion = canonicalVersion;
    mVersionPattern = Pattern.compile(versionPattern);
  }

  /**
   * @param versionString given version string
   * @return the corresponding {@link HdfsVersion} instance
   */
  @Nullable
  public static HdfsVersion find(String versionString) {
    for (HdfsVersion version : HdfsVersion.values()) {
      if (version.mVersionPattern.matcher(versionString).matches()) {
        return version;
      }
    }
    return null;
  }

  /**
   * @param versionA base version
   * @param versionB version to compare
   * @return if the two versions match explicitly or by pattern
   */
  public static boolean matches(String versionA, String versionB) {
    if (versionA.equals(versionB)) {
      return true;
    }
    if (HdfsVersion.find(versionA) != null
        && HdfsVersion.find(versionA) == HdfsVersion.find(versionB)) {
      return true;
    }
    return false;
  }

  /**
   * @return the canonical version string
   */
  public String getCanonicalVersion() {
    return mCanonicalVersion;
  }
}
