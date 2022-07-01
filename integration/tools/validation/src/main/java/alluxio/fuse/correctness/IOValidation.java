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

package alluxio.fuse.correctness;

/**
 * This class is the entry class for fuse correctness validation tool.
 */
public class IOValidation {
  /**
   * This main function is the entry point for fuse correctness validation.
   * @param args command line args
   */
  public static void main(String[] args) {
    Options options = Options.createOptions(args);
    switch (options.getOperation()) {
      case Read:
        ReadValidation.run(options);
        break;
      case Write:
        WriteValidation.run(options);
        break;
      default:
        break;
    }
  }
}
