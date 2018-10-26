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

import alluxio.annotation.PublicApi;
import alluxio.proto.journal.File;
import alluxio.thrift.TSetAclAction;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Represents the possible actions for setting ACL.
 */
@PublicApi
@ThreadSafe
public enum SetAclAction {

  /** The specified ACL entries will fully replace the existing ACL. */
  REPLACE,

  /** The specified ACL entries added. */
  MODIFY,

  /** The specified ACL entries will be removed. */
  REMOVE,

  /** All existing ACL entries will be removed, except for the base ACL entries. */
  REMOVE_ALL,

  /** The default ACL entries will be removed. */
  REMOVE_DEFAULT,
  ;

  /**
   * Converts proto type to wire type.
   *
   * @param pSetAclAction {@link TSetAclAction}
   * @return {@link SetAclAction} equivalent
   */
  public static SetAclAction fromProto(File.SetAclAction pSetAclAction) {
    if (pSetAclAction == null) {
      throw new IllegalStateException("Null proto set acl action.");
    }
    switch (pSetAclAction) {
      case REPLACE:
        return REPLACE;
      case MODIFY:
        return MODIFY;
      case REMOVE:
        return REMOVE;
      case REMOVE_ALL:
        return REMOVE_ALL;
      case REMOVE_DEFAULT:
        return REMOVE_DEFAULT;
      default:
        throw new IllegalStateException("Unrecognized proto set acl action: " + pSetAclAction);
    }
  }

  /**
   * Converts wire type to thrift type.
   *
   * @return {@link TSetAclAction} equivalent
   */
  public File.SetAclAction toProto() {
    switch (this) {
      case REPLACE:
        return File.SetAclAction.REPLACE;
      case MODIFY:
        return File.SetAclAction.MODIFY;
      case REMOVE:
        return File.SetAclAction.REMOVE;
      case REMOVE_ALL:
        return File.SetAclAction.REMOVE_ALL;
      case REMOVE_DEFAULT:
        return File.SetAclAction.REMOVE_DEFAULT;
      default:
        throw new IllegalStateException("Unrecognized set acl action: " + this);
    }
  }

  /**
   * Converts thrift type to wire type.
   *
   * @param tSetAclAction {@link TSetAclAction}
   * @return {@link SetAclAction} equivalent
   */
  public static SetAclAction fromThrift(TSetAclAction tSetAclAction) {
    if (tSetAclAction == null) {
      throw new IllegalStateException("Null thrift set acl action.");
    }
    switch (tSetAclAction) {
      case Replace:
        return REPLACE;
      case Modify:
        return MODIFY;
      case Remove:
        return REMOVE;
      case RemoveAll:
        return REMOVE_ALL;
      case RemoveDefault:
        return REMOVE_DEFAULT;
      default:
        throw new IllegalStateException("Unrecognized thrift set acl action: " + tSetAclAction);
    }
  }

  /**
   * Converts wire type to thrift type.
   *
   * @return {@link TSetAclAction} equivalent
   */
  public TSetAclAction toThrift() {
    switch (this) {
      case REPLACE:
        return TSetAclAction.Replace;
      case MODIFY:
        return TSetAclAction.Modify;
      case REMOVE:
        return TSetAclAction.Remove;
      case REMOVE_ALL:
        return TSetAclAction.RemoveAll;
      case REMOVE_DEFAULT:
        return TSetAclAction.RemoveDefault;
      default:
        throw new IllegalStateException("Unrecognized set acl action: " + this);
    }
  }
}
