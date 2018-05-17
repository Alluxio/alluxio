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

import alluxio.thrift.TAclEntryType;

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

  /**
   * @return the string representation for the CLI
   */
  public String toCliString() {
    switch (this) {
      case OWNING_USER:
      case NAMED_USER:
        return "user";
      case OWNING_GROUP:
      case NAMED_GROUP:
        return "group";
      case MASK:
        return "mask";
      case OTHER:
        return "other";
      default:
        throw new IllegalStateException("Unknown AclEntryType: " + this);
    }
  }

  /**
   * @param tAclEntryType the thrift representation
   * @return the {@link AclEntryType} created from the thrift representation
   */
  public static AclEntryType fromThrift(TAclEntryType tAclEntryType) {
    switch (tAclEntryType) {
      case Owner:
        return OWNING_USER;
      case NamedUser:
        return NAMED_USER;
      case OwningGroup:
        return OWNING_GROUP;
      case NamedGroup:
        return NAMED_GROUP;
      case Mask:
        return MASK;
      case Other:
        return OTHER;
      default:
        throw new IllegalStateException("Unknown TAclEntryType: " + tAclEntryType);
    }
  }

  /**
   * @return the thrift representation of this enum
   */
  public TAclEntryType toThrift() {
    switch (this) {
      case OWNING_USER:
        return TAclEntryType.Owner;
      case NAMED_USER:
        return TAclEntryType.NamedUser;
      case OWNING_GROUP:
        return TAclEntryType.OwningGroup;
      case NAMED_GROUP:
        return TAclEntryType.NamedGroup;
      case MASK:
        return TAclEntryType.Mask;
      case OTHER:
        return TAclEntryType.Other;
      default:
        throw new IllegalStateException("Unknown AclEntryType: " + this);
    }
  }
}
