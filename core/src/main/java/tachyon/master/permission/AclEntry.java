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

    private final static AclPermission[] vals = values();

    public final String SYMBOL;

    private AclPermission(String s) {
      SYMBOL = s;
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
      return vals[ordinal() & that.ordinal()];
    }

    /** OR operation. */
    public AclPermission or(AclPermission that) {
      return vals[ordinal() | that.ordinal()];
    }

    /** NOT operation. */
    public AclPermission not() {
      return vals[7 - ordinal()];
    }

  }

  private final AclType type;
  private String name;
  private AclPermission permission;

  private AclEntry(AclType type, String name, AclPermission permission) {
    this.type = type;
    this.name = name;
    this.permission = permission;
  }

  public AclType getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public AclPermission getPermission() {
    return permission;
  }

  public void setPermission(AclPermission permission) {
    this.permission = permission;
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
    return Objects.equal(type, other.type) &&
      Objects.equal(name, other.name) &&
      Objects.equal(permission, other.permission);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(type, name, permission);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (type != null) {
      sb.append(type.toString().toLowerCase());
    }
    sb.append(':');
    if (name != null) {
      sb.append(name);
    }
    sb.append(':');
    if (permission != null) {
      sb.append(permission.toString());
    }
    return sb.toString();
  }

  @Override
  public int compareTo(AclEntry other) {
    return ComparisonChain.start()
        .compare(type, other.type, Ordering.explicit(AclType.USER, AclType.GROUP, AclType.OTHER))
        .result();
  }

  /**
   * Builder for creating new AclEntry instances.
   */
  public static class Builder {
    private AclType type;
    private String name;
    private AclPermission permission;

    public Builder setType(AclType type) {
      this.type = type;
      return this;
    }

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setPermission(AclPermission permission) {
      this.permission = permission;
      return this;
    }

    public AclEntry build() {
      return new AclEntry(type, name, permission);
    }
  }
}
