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

package alluxio.cli;

import alluxio.PropertyKey;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Utility for printing Alluxio configuration key.
 */
public final class GetConfKey {
  private static final String USAGE = "USAGE: GetConfKey [ENV_VARIABLE_NAME]\n\n"
      + "GetConfKey prints the configuration key given an environment variable name. "
      + "If the key is invalid, the exit code will be nonzero.";

  private static final Map<String, String> ENV_VIOLATORS = ImmutableMap.<String, String>builder()
      .put("ALLUXIO_UNDERFS_S3A_INHERIT_ACL", "alluxio.underfs.s3a.inherit_acl")
      .put("ALLUXIO_MASTER_FORMAT_FILE_PREFIX", "alluxio.master.format.file_prefix")
      .put("AWS_ACCESSKEYID", "aws.accessKeyId")
      .put("AWS_SECRETKEY", "aws.secretKey")
      .put("FS_GCS_ACCESSKEYID", "fs.gcs.accessKeyId")
      .put("FS_GCS_SECRETACCESSKEY", "fs.gcs.secretAccessKey")
      .put("FS_OSS_ACCESSKEYID", "fs.oss.accessKeyId")
      .put("FS_OSS_ACCESSKEYSECRET", "fs.oss.accessKeySecret")
      .build();

  /**
   * Prints the help message.
   *
   * @param message message before standard usage information
   */
  public static void printHelp(String message) {
    System.err.println(message);
    System.out.println(USAGE);
  }

  /**
   * Implements get configuration key.
   *
   * @param args the arguments to specify the environment variable name
   * @return 0 on success, 1 on failures
   */
  public static int getConfKey(String... args) {
    switch (args.length) {
      case 0:
        printHelp("Missing argument.");
        return 1;
      case 1:
        String varName = args[0].trim();
        String propertyName = ENV_VIOLATORS.getOrDefault(varName,
            varName.toLowerCase().replace("_", "."));
        if (!PropertyKey.isValid(propertyName)) {
          printHelp(String.format("%s is not a valid configuration key", propertyName));
          return 1;
        }
        PropertyKey key = PropertyKey.fromString(propertyName);
        System.out.println(key.getName());
        break;
      default:
        printHelp("More arguments than expected");
        return 1;
    }
    return 0;
  }

  /**
   * Prints Alluxio configuration key.
   *
   * @param args the arguments to specify the environment variable name
   */
  public static void main(String[] args) {
    System.exit(getConfKey(args));
  }

  private GetConfKey() {} // this class is not intended for instantiation
}
