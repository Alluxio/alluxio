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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import tachyon.master.permission.AclEntry.AclPermission;

import com.google.common.base.Objects;


/**
 * This class contains three {@link AclEntry}: userEncry, groupEntry, and otherEntry.
 */
public class Acl {

  //POSIX aclEntry
  private AclEntry userEncry;
  private AclEntry groupEntry;
  private AclEntry otherEntry;

  private static final Log LOG = LogFactory.getLog(Acl.class);

  /**
   * Construct by the given {@link AclEntry}.
   * @param u, user aclEntry
   * @param g, group aclEntry
   * @param o, other aclEntry
   */
  private Acl(AclEntry u, AclEntry g, AclEntry o) {
    this.userEncry = u;
    this.groupEntry = g;
    this.otherEntry = o;
  }

  /**
   * Copy constructor
   *
   * @param other, other permission
   */
  public Acl(Acl other) {
    this.userEncry = other.userEncry;
    this.groupEntry = other.groupEntry;
    this.otherEntry = other.otherEntry;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (getClass() != o.getClass()) {
      return false;
    }
    Acl other = (Acl)o;
    return Objects.equal(userEncry, other.userEncry) &&
      Objects.equal(groupEntry, other.groupEntry) &&
      Objects.equal(otherEntry, other.otherEntry);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(userEncry, groupEntry, otherEntry);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append('{');
    if (userEncry != null) {
      sb.append('[')
        .append(userEncry.toString())
        .append(']');
    }
    sb.append(',');
    if (groupEntry != null) {
      sb.append('[')
        .append(groupEntry.toString())
        .append(']');
    }
    sb.append(',');
    if (otherEntry != null) {
      sb.append('[')
        .append(otherEntry.toString())
        .append(']');
    }
    sb.append('}');
    return sb.toString();
  }

  /**
   * Retrun short of this ACL permission
   */
  public short toShort() {
    int s = (getUserPermission().ordinal() << 6)  |
            (getGroupPermission().ordinal() << 3) |
            getOtherPermission().ordinal();
    return (short)s;
  }


  public AclPermission getUserPermission() {
    return userEncry.getPermission();
  }

  public AclPermission getGroupPermission() {
    return groupEntry.getPermission();
  }

  public AclPermission getOtherPermission() {
    return otherEntry.getPermission();
  }

  public String getUserName() {
    return userEncry.getName();
  }

  public String getGroupName() {
    return groupEntry.getName();
  }

  public void umask(Acl umask) {
    getUserPermission().and(umask.getUserPermission().not());
    getGroupPermission().and(umask.getGroupPermission().not());
    getOtherPermission().and(umask.getOtherPermission().not());
  }

  /**
   * Umask from a conf, umask should set to conf with key "tfs.permission.umask"
   *
   * @param conf
   */
  public void umask(Configuration conf) {
    umask(AclUtil.getUMask(conf));
  }

  /**
   * Umask from a short, e.g. umask 0002
   *
   * @param n, a short of umask
   */
  public void umask(short n) {
    AclPermission userUmask = AclUtil.toUserPermission(n);
    AclPermission groupUmask = AclUtil.toGroupPermission(n);
    AclPermission otherUmask = AclUtil.toOtherPermission(n);
    AclPermission newUserPermission = getUserPermission().and(userUmask.not());
    AclPermission newGroupPermission = getGroupPermission().and(groupUmask.not());
    AclPermission newOtherPermission = getOtherPermission().and(otherUmask.not());
    setPermission(newUserPermission, newGroupPermission, newOtherPermission);
  }

  /**
   * Set permission to AclEntry from a short
   * Used for chmod, e.g. chmod 777
   *
   * @param n, a short of permission
   */
  public void setPermission(short n) {
    setPermission(
        AclUtil.toUserPermission(n),
        AclUtil.toGroupPermission(n),
        AclUtil.toOtherPermission(n));
  }

  private void setPermission(AclPermission u, AclPermission g, AclPermission o) {
    userEncry.setPermission(u);
    groupEntry.setPermission(g);
    otherEntry.setPermission(o);
  }

  /**
   * Set user's owner from a given name
   * User for chown
   *
   * @param name, new owner name
   */
  public void setUserOwner(String name) {
    userEncry.setName(name);
  }

  /**
   * Set group's owner from a given name
   * User for chown
   *
   * @param name, new owner name
   */
  public void setGroupOwner(String name) {
    groupEntry.setName(name);
  }

  /**
   * Builder for creating new ACL instances.
   */
  public static class Builder {
    private AclEntry userEntry;
    private AclEntry groupEntry;
    private AclEntry otherEntry;

    public Builder setUserEntry(AclEntry u) {
      this.userEntry = u;
      return this;
    }

    public Builder setGroupEntry(AclEntry g) {
      this.groupEntry = g;
      return this;
    }

    public Builder setOtherEntry(AclEntry o) {
      this.otherEntry = o;
      return this;
    }

    public Acl build() {
      return new Acl(userEntry, groupEntry, otherEntry);
    }
  }
}