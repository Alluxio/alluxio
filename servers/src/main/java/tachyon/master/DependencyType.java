/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master;

import java.io.IOException;

/**
 * The type of Dependency, Wide and Narrow. The difference is that how many input files does each
 * output file require in the Dependency. In Narrow Dependency, each output file only requires one
 * input file, while in Wide Dependency it requires more than one.
 */
public enum DependencyType {
  /**
   * Wide Dependency means that each output file requires more than one input files.
   */
  Wide(1),
  /**
   * Narrow Dependency means that each output file only requires one input file.
   */
  Narrow(2);

  /**
   * Get the enum value with the given integer value. It will check the legality.
   *
   * @param value The integer value, 1 or 2, means Wide or Narrow
   * @return the enum value of DependencyType
   * @throws IOException when unknown dependency type is encountered
   */
  public static DependencyType getDependencyType(int value) throws IOException {
    if (value == 1) {
      return Wide;
    } else if (value == 2) {
      return Narrow;
    }

    throw new IOException("Unknown DependencyType value " + value);
  }

  private final int mValue;

  private DependencyType(int value) {
    mValue = value;
  }

  /**
   * Get the integer value of this enum value.
   *
   * @return integer value of the dependency type
   */
  public int getValue() {
    return mValue;
  }
}
