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

package alluxio.wire;

import java.util.Arrays;

/**
 * Thrown when the version is invalid and thus not parsable.
 */
public class InvalidVersionParsingException extends ProtoParsingException {

  /**
   * Constructs a new exception with details of the invalid version.
   * @param actualVersion
   * @param expectedVersion
   */
  public InvalidVersionParsingException(int actualVersion, int... expectedVersion) {
    super(String.format("The proto message has a version %s that cannot be parsed "
        + "by a parser expecting versions %s", actualVersion, Arrays.toString(expectedVersion)));
  }
}
