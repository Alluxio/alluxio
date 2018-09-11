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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Stores the extended ACL entries.
 */
@NotThreadSafe
public class ExtendedACLEntries {
  protected Map<String, AclActions> mNamedUserActions;
  protected Map<String, AclActions> mNamedGroupActions;
  protected AclActions mMaskActions;

  /**
   * Creates an empty extended ACL.
   */
  public ExtendedACLEntries() {
    mNamedUserActions = new TreeMap<>();
    mNamedGroupActions = new TreeMap<>();
    mMaskActions = new AclActions();
  }

  /**
   * Creates a copy.
   *
   * @param other the object to copy
   */
  public ExtendedACLEntries(ExtendedACLEntries other) {
    mNamedUserActions = new TreeMap<>(other.mNamedUserActions);
    mNamedGroupActions = new TreeMap<>(other.mNamedGroupActions);
    mMaskActions = new AclActions(other.mMaskActions);
  }

  /**
   * Returns a list of {@link AclEntry} which represent this ACL instance. The mask will only be
   * included if extended ACL entries exist.
   *
   * @return an immutable list of ACL entries
   */
  public List<AclEntry> getEntries() {
    ImmutableList.Builder<AclEntry> builder = new ImmutableList.Builder<>();
    for (Map.Entry<String, AclActions> kv : mNamedUserActions.entrySet()) {
      builder.add(new AclEntry.Builder()
          .setType(AclEntryType.NAMED_USER)
          .setSubject(kv.getKey())
          .setActions(kv.getValue())
          .build());
    }
    for (Map.Entry<String, AclActions> kv : mNamedGroupActions.entrySet()) {
      builder.add(new AclEntry.Builder()
          .setType(AclEntryType.NAMED_GROUP)
          .setSubject(kv.getKey())
          .setActions(kv.getValue())
          .build());
    }
    if (hasExtended()) {
      // The mask is only relevant if the ACL contains extended entries.
      builder.add(new AclEntry.Builder()
          .setType(AclEntryType.MASK)
          .setActions(mMaskActions)
          .build());
    }
    return builder.build();
  }

  /**
   * @return named user actions
   */
  public Map<String, AclActions> getNamedUserActions() {
    return mNamedUserActions;
  }

  /**
   * @return named group actions
   */
  public Map<String, AclActions> getNamedGroupActions() {
    return mNamedGroupActions;
  }

  /**
   * @return true if has extended ACL (named users, named groups)
   */
  public boolean hasExtended() {
    return !mNamedUserActions.isEmpty() || !mNamedGroupActions.isEmpty();
  }

  /**
   * Removes the specified entry. A base entry is not allowed to be removed.
   *
   * @param entry the entry to be removed
   */
  public void removeEntry(AclEntry entry) {
    switch (entry.getType()) {
      case NAMED_USER:
        mNamedUserActions.remove(entry.getSubject());
        return;
      case NAMED_GROUP:
        mNamedGroupActions.remove(entry.getSubject());
        return;
      case MASK:
        if (hasExtended()) {
          throw new IllegalStateException(
              "Deleting the mask for extended ACLs is not allowed. entry: " + entry);
        } else {
          mMaskActions = new AclActions();
        }
        return;
      case OWNING_USER:  // fall through
      case OWNING_GROUP: // fall through
      case OTHER:
        throw new IllegalStateException(
            "Deleting base entry is not allowed. entry: " + entry);
      default:
        throw new IllegalStateException("Unknown ACL entry type: " + entry.getType());
    }
  }

  /**
   * Sets an entry into the access control list.
   * If an entry with the same type and subject already exists, overwrites the existing entry;
   * Otherwise, adds this new entry.
   *
   * @param entry the entry to be added or updated
   */
  public void setEntry(AclEntry entry) {
    // TODO(cc): when setting non-mask entries, the mask should be dynamically updated too.
    switch (entry.getType()) {
      case NAMED_USER:
        mNamedUserActions.put(entry.getSubject(), entry.getActions());
        return;
      case NAMED_GROUP:
        mNamedGroupActions.put(entry.getSubject(), entry.getActions());
        return;
      case MASK:
        mMaskActions = entry.getActions();
        return;
      case OWNING_USER:  // fall through
      case OWNING_GROUP: // fall through
      case OTHER:
        throw new IllegalStateException(
            "Deleting base entry is not allowed. entry: " + entry);
      default:
        throw new IllegalStateException("Unknown ACL entry type: " + entry.getType());
    }
  }

  /**
   * @param user the user to look up
   * @return the actions for the user, or null if the user is not found
   */
  public AclActions getNamedUser(String user) {
    return mNamedUserActions.get(user);
  }

  /**
   * @param group the group to look up
   * @return the actions for the group, or null if the group is not found
   */
  public AclActions getNamedGroup(String group) {
    return mNamedGroupActions.get(group);
  }

  /**
   * @return the mask actions
   */
  public AclActions getMask() {
    return mMaskActions;
  }

  /**
   * Update the mask to be the union of owning group entry, named user entry and named group entry.
   * @param groupActions the group entry to be integrated into the mask
   */
  public void updateMask(AclActions groupActions) {
    AclActions result = new AclActions(groupActions);

    for (Map.Entry<String, AclActions> kv : mNamedUserActions.entrySet()) {
      AclActions userAction = kv.getValue();
      result.merge(userAction);

      for (AclAction action : AclAction.values()) {
        if (result.contains(action) || userAction.contains(action)) {
          result.add(action);
        }
      }
    }

    for (Map.Entry<String, AclActions> kv : mNamedGroupActions.entrySet()) {
      AclActions userAction = kv.getValue();
      result.merge(userAction);

      for (AclAction action : AclAction.values()) {
        if (result.contains(action) || userAction.contains(action)) {
          result.add(action);
        }
      }
    }

    mMaskActions = result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ExtendedACLEntries)) {
      return false;
    }
    ExtendedACLEntries that = (ExtendedACLEntries) o;
    return mNamedUserActions.equals(that.mNamedUserActions)
        && mNamedGroupActions.equals(that.mNamedGroupActions)
        && mMaskActions.equals(that.mMaskActions);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mNamedUserActions, mNamedGroupActions, mMaskActions);
  }
}
