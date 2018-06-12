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

import java.io.IOException;
import java.util.List;

/**
 * Default Access control list for a directory.
 */
public class DefaultAccessControlList extends AccessControlList {
  private AccessControlList mAccessAcl;

  /**
   * Constructor to build a default ACL based on an access ACL.
   * @param acl the access acl
   */
  public  DefaultAccessControlList(AccessControlList acl) {
    super();
    mAccessAcl = acl;
    mOwningUser = acl.mOwningUser;
    mOwningGroup = acl.mOwningGroup;
    mUserActions.put(OWNING_USER_KEY, new AclActions(acl.mUserActions.get(OWNING_USER_KEY)));
    mGroupActions.put(OWNING_GROUP_KEY, new AclActions(acl.mGroupActions.get(OWNING_GROUP_KEY)));
    mOtherActions = new AclActions(acl.mOtherActions);
  }

  private void fillDefault() {
    if (mUserActions.get(OWNING_USER_KEY) == null) {
      mUserActions.put(OWNING_USER_KEY,
          new AclActions(mAccessAcl.mUserActions.get(OWNING_USER_KEY)));
    }
    if (mGroupActions.get(OWNING_GROUP_KEY) == null) {
      mGroupActions.put(OWNING_GROUP_KEY,
          new AclActions(mAccessAcl.mGroupActions.get(OWNING_GROUP_KEY)));
    }
    if (mOtherActions == null) {
      mOtherActions = new AclActions(mAccessAcl.mOtherActions);
    }
  }

  /**
   * Removes the specified entry. A base entry is not allowed to be removed.
   *
   * @param entry the entry to be removed
   */
  @Override
  public void removeEntry(AclEntry entry) throws IOException {
    switch (entry.getType()) {
      case NAMED_USER:
        mUserActions.remove(entry.getSubject());
        return;
      case NAMED_GROUP:
        mGroupActions.remove(entry.getSubject());
        return;
      case MASK:
        if (hasExtended()) {
          // cannot remove the mask if it is extended.
          throw new IOException(
              "Deleting the mask for extended ACLs is not allowed. entry: " + entry);
        } else {
          mMaskActions = new AclActions();
        }
        return;
      case OWNING_USER:
        mUserActions.remove(entry.getSubject());
        fillDefault();
        return;
      case OWNING_GROUP:
        mGroupActions.remove(entry.getSubject());
        fillDefault();
        return;
      case OTHER:
        mOtherActions = null;
        fillDefault();
        return;
      default:
        throw new IllegalStateException("Unknown ACL entry type: " + entry.getType());
    }
  }

  /**
   * Returns a list of {@link AclEntry} which represent this ACL instance. The mask will only be
   * included if extended ACL entries exist.
   *
   * @return an immutable list of ACL entries
   */
  @Override
  public List<AclEntry> getEntries() {
    List<AclEntry> aclEntryList = super.getEntries();
    for (AclEntry entry : aclEntryList) {
      entry.setDefault(true);
    }
    return aclEntryList;
  }
}
