package alluxio.security.authorization;


import java.io.IOException;
import java.util.List;

public class DefaultAccessControlList extends AccessControlList {
  private AccessControlList mAccessAcl;

  public  DefaultAccessControlList(AccessControlList acl) {
    super();
    mAccessAcl = acl;
    mOwningUser = acl.mOwningUser;
    mOwningGroup = acl.mOwningGroup;
    mUserActions.put(OWNING_USER_KEY, new AclActions(acl.mUserActions.get(OWNING_USER_KEY)));
    mGroupActions.put(OWNING_GROUP_KEY, new AclActions(acl.mGroupActions.get(OWNING_GROUP_KEY)));
    mOtherActions = new AclActions(acl.mOtherActions);
  }

  public void fillDefault() {
    if (mUserActions.get(OWNING_USER_KEY) == null) {
      mUserActions.put(OWNING_USER_KEY, new AclActions(mAccessAcl.mUserActions.get(OWNING_USER_KEY)));
    }
    if (mGroupActions.get(OWNING_GROUP_KEY) == null) {
      mGroupActions.put(OWNING_GROUP_KEY, new AclActions(mAccessAcl.mGroupActions.get(OWNING_GROUP_KEY)));
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

  public List<AclEntry> getEntries() {
    List<AclEntry> aclEntryList = super.getEntries();
    for (AclEntry entry : aclEntryList) {
      entry.setDefault(true);
    }
    return aclEntryList;
  }

}
