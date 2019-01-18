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

package alluxio.security.authorization;

/**
 * Actions to be controlled in {@link AccessControlList}.
 */
public enum AclAction {
  READ,
  WRITE,
  EXECUTE;

  /** AclAction values. */
  private static final AclAction[] VALUES = AclAction.values();

  /**
   * @param ordinal the ordinal of the target action in {@link AclAction}
   * @return the {@link AclAction} with the specified ordinal
   * @throws IndexOutOfBoundsException when ordinal is out of range of valid ordinals
   */
  public static AclAction ofOrdinal(int ordinal) {
    return VALUES[ordinal];
  }
}
