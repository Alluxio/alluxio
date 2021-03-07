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
 * Types of entries in {@link AccessControlList}.
 */
public enum AclEntryType {
  OWNING_USER,
  NAMED_USER,
  OWNING_GROUP,
  NAMED_GROUP,
  MASK,
  OTHER;

  public static final String USER_COMPONENT = "user";
  public static final String GROUP_COMPONENT = "group";
  public static final String MASK_COMPONENT = "mask";
  public static final String OTHER_COMPONENT = "other";

  /**
   * @return the string representation for the CLI
   */
  public String toCliString() {
    switch (this) {
      case OWNING_USER:
      case NAMED_USER:
        return USER_COMPONENT;
      case OWNING_GROUP:
      case NAMED_GROUP:
        return GROUP_COMPONENT;
      case MASK:
        return MASK_COMPONENT;
      case OTHER:
        return OTHER_COMPONENT;
      default:
        throw new IllegalStateException("Unknown AclEntryType: " + this);
    }
  }
}
