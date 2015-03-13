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

package tachyon.master.permission;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;

/**
 * POSIX AclEntry, which include aclType, name and aclPermission
 * aclType, type is {@link AclType}, used to present user/group/other
 * name, type is String, used to present name of user/group
 * aclPermission, type is {@link AclPermission}, used to present permission(rwx) of this aclEntry
 */
public class AclEntry implements Comparable<AclEntry> {

  /**
   * AclType, used to present user/group/other
   */
  public enum AclType {
    USER,
    GROUP,
    OTHER;
  }

  /**
   * AclPermission, used to present rwx
   */
  public enum AclPermission {
    NONE("---"),
    EXECUTE("--x"),
    WRITE("-w-"),
    WRITE_EXECUTE("-wx"),
    READ("r--"),
    READ_EXECUTE("r-x"),
    READ_WRITE("rw-"),
    ALL("rwx");

    private static final AclPermission[] VALS = values();

    public final String mValue;

    private AclPermission(String s) {
      mValue = s;
    }

    /**
     * Return true if this permission implies that permission.
     * @param that
     */
    public boolean implies(AclPermission that) {
      if (that != null) {
        return (ordinal() & that.ordinal()) == that.ordinal();
      }
      return false;
    }

    /** AND operation. */
    public AclPermission and(AclPermission that) {
      return VALS[ordinal() & that.ordinal()];
    }

    /** OR operation. */
    public AclPermission or(AclPermission that) {
      return VALS[ordinal() | that.ordinal()];
    }

    /** NOT operation. */
    public AclPermission not() {
      return VALS[7 - ordinal()];
    }

  }

  private final AclType mType;
  private String mName;
  private AclPermission mPermission;

  private AclEntry(AclType type, String name, AclPermission permission) {
    this.mType = type;
    this.mName = name;
    this.mPermission = permission;
  }

  public AclType getType() {
    return mType;
  }

  public String getName() {
    return mName;
  }

  public void setName(String name) {
    this.mName = name;
  }

  public AclPermission getPermission() {
    return mPermission;
  }

  public void setPermission(AclPermission permission) {
    this.mPermission = permission;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (getClass() != o.getClass()) {
      return false;
    }
    AclEntry other = (AclEntry)o;
    return Objects.equal(mType, other.mType)
        && Objects.equal(mName, other.mName)
        && Objects.equal(mPermission, other.mPermission);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mType, mName, mPermission);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (mType != null) {
      sb.append(mType.toString().toLowerCase());
    }
    sb.append(':');
    if (mName != null) {
      sb.append(mName);
    }
    sb.append(':');
    if (mPermission != null) {
      sb.append(mPermission.toString());
    }
    return sb.toString();
  }

  @Override
  public int compareTo(AclEntry other) {
    return ComparisonChain.start()
        .compare(mType, other.mType, Ordering.explicit(AclType.USER, AclType.GROUP, AclType.OTHER))
        .result();
  }

  /**
   * Builder for creating new AclEntry instances.
   */
  public static class Builder {
    private AclType mType;
    private String mName;
    private AclPermission mPermission;

    public Builder setType(AclType type) {
      this.mType = type;
      return this;
    }

    public Builder setName(String name) {
      this.mName = name;
      return this;
    }

    public Builder setPermission(AclPermission permission) {
      this.mPermission = permission;
      return this;
    }

    public AclEntry build() {
      return new AclEntry(mType, mName, mPermission);
    }
  }
}
