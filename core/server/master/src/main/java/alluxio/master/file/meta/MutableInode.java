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

package alluxio.master.file.meta;

import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.TtlAction;
import alluxio.master.ProtobufUtils;
import alluxio.proto.journal.File.UpdateInodeEntry;
import alluxio.proto.journal.Journal;
import alluxio.proto.meta.InodeMeta;
import alluxio.proto.meta.InodeMeta.InodeOrBuilder;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.AclAction;
import alluxio.security.authorization.AclActions;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.AclEntryType;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.util.CommonUtils;
import alluxio.util.proto.ProtoUtils;
import alluxio.wire.FileInfo;

import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * {@link MutableInode} is an abstract class, with information shared by all types of Inodes.
 *
 * @param <T> the concrete subclass of this object
 */
@NotThreadSafe
public abstract class MutableInode<T extends MutableInode> implements InodeView {
  private static final Logger LOG = LoggerFactory.getLogger(MutableInode.class);
  protected long mCreationTimeMs;
  private boolean mDeleted;
  protected final boolean mDirectory;
  protected final long mId;
  protected long mTtl;
  protected TtlAction mTtlAction;
  private long mLastModificationTimeMs;
  private long mLastAccessTimeMs;
  private String mName;
  private long mParentId;
  private PersistenceState mPersistenceState;
  private boolean mPinned;
  private Set<String> mMediumTypes;
  protected AccessControlList mAcl;
  private String mUfsFingerprint;
  private Map<String, byte[]> mXAttr;

  protected MutableInode(long id, boolean isDirectory) {
    mCreationTimeMs = System.currentTimeMillis();
    mDeleted = false;
    mDirectory = isDirectory;
    mId = id;
    mTtl = Constants.NO_TTL;
    mTtlAction = TtlAction.DELETE;
    mLastModificationTimeMs = mCreationTimeMs;
    mLastAccessTimeMs = mCreationTimeMs;
    mName = null;
    mParentId = InodeTree.NO_PARENT;
    mPersistenceState = PersistenceState.NOT_PERSISTED;
    mPinned = false;
    mMediumTypes = new HashSet<>();
    mAcl = new AccessControlList();
    mUfsFingerprint = Constants.INVALID_UFS_FINGERPRINT;
    mXAttr = null;
  }

  @Override
  public long getCreationTimeMs() {
    return mCreationTimeMs;
  }

  @Override
  public String getGroup() {
    return mAcl.getOwningGroup();
  }

  @Override
  public long getId() {
    return mId;
  }

  @Override
  public long getTtl() {
    return mTtl;
  }

  @Override
  public TtlAction getTtlAction() {
    return mTtlAction;
  }

  @Override
  public long getLastModificationTimeMs() {
    return mLastModificationTimeMs;
  }

  @Override
  public long getLastAccessTimeMs() {
    return mLastAccessTimeMs;
  }

  @Override
  public String getName() {
    return mName;
  }

  @Override
  public short getMode() {
    return mAcl.getMode();
  }

  @Override
  public PersistenceState getPersistenceState() {
    return mPersistenceState;
  }

  @Override
  public long getParentId() {
    return mParentId;
  }

  @Override
  public String getOwner() {
    return mAcl.getOwningUser();
  }

  @Override
  public boolean isDeleted() {
    return mDeleted;
  }

  @Override
  public boolean isDirectory() {
    return mDirectory;
  }

  @Override
  public boolean isFile() {
    return !mDirectory;
  }

  @Override
  public boolean isPinned() {
    return mPinned;
  }

  @Override
  public boolean isPersisted() {
    return mPersistenceState == PersistenceState.PERSISTED;
  }

  @Override
  public String getUfsFingerprint() {
    return mUfsFingerprint;
  }

  @Override
  public AccessControlList getACL() {
    return mAcl;
  }

  @Override
  @Nullable
  public Map<String, byte[]> getXAttr() {
    return mXAttr;
  }

  @Override
  public Set<String> getMediumTypes() {
    return mMediumTypes;
  }

  /**
   * Removes the extended ACL entries. The base entries are retained.
   *
   * @return the updated object
   */
  public T removeExtendedAcl() {
    mAcl.removeExtendedEntries();
    return getThis();
  }

  /**
   * Removes ACL entries.
   *
   * @param entries the ACL entries to remove
   * @return the updated object
   */
  public T removeAcl(List<AclEntry> entries) {
    for (AclEntry entry : entries) {
      if (entry.isDefault()) {
        AccessControlList defaultAcl = getDefaultACL();
        defaultAcl.removeEntry(entry);
      } else {
        mAcl.removeEntry(entry);
      }
    }
    updateMask(entries);
    return getThis();
  }

  /**
   * Replaces all existing ACL entries with a new list of entries.
   *
   * @param entries the new list of ACL entries
   * @return the updated object
   */
  public T replaceAcl(List<AclEntry> entries) {
    boolean clearACL = false;
    for (AclEntry entry : entries) {
      /**
       * if we are only setting default ACLs, we do not need to clear access ACL entries
       * observed same behavior on linux
       */
      if (!entry.isDefault()) {
        clearACL = true;
      }
    }
    if (clearACL) {
      mAcl.clearEntries();
    }
    return setAcl(entries);
  }

  /**
   * Update Mask for the Inode.
   * This method should be called after updates to ACL and defaultACL.
   *
   * @param entries the list of ACL entries
   * @return the updated object
   */
  public T updateMask(List<AclEntry> entries) {
    boolean needToUpdateACL = false;
    boolean needToUpdateDefaultACL = false;

    for (AclEntry entry : entries) {
      if (entry.getType().equals(AclEntryType.NAMED_USER)
          || entry.getType().equals(AclEntryType.NAMED_GROUP)
          || entry.getType().equals(AclEntryType.OWNING_GROUP)) {
        if (entry.isDefault()) {
          needToUpdateDefaultACL = true;
        } else {
          needToUpdateACL = true;
        }
      }
      if (entry.getType().equals(AclEntryType.MASK)) {
        // If mask is explicitly set or removed then we don't need to update the mask
        return getThis();
      }
    }
    if (needToUpdateACL) {
      mAcl.updateMask();
    }

    if (needToUpdateDefaultACL) {
      getDefaultACL().updateMask();
    }
    return getThis();
  }

  /**
   * @param creationTimeMs the creation time to use (in milliseconds)
   * @return the updated object
   */
  public T setCreationTimeMs(long creationTimeMs) {
    mCreationTimeMs = creationTimeMs;
    return getThis();
  }

  /**
   * @param deleted the deleted flag to use
   * @return the updated object
   */
  public T setDeleted(boolean deleted) {
    mDeleted = deleted;
    return getThis();
  }

  /**
   * @param group the group of the inode
   * @return the updated object
   */
  public T setGroup(String group) {
    mAcl.setOwningGroup(group);
    if (isDirectory()) {
      getDefaultACL().setOwningGroup(group);
    }
    return getThis();
  }

  /**
   * Sets the last modification time of the inode to the new time if the new time is more recent.
   * This method can be called concurrently with deterministic results.
   *
   * @param lastModificationTimeMs the last modification time to use
   * @return the updated object
   */
  public T setLastModificationTimeMs(long lastModificationTimeMs) {
    return setLastModificationTimeMs(lastModificationTimeMs, false);
  }

  /**
   * @param lastModificationTimeMs the last modification time to use
   * @param override if true, sets the value regardless of the previous last modified time,
   *                 should be set to true for journal replay
   * @return the updated object
   */
  public T setLastModificationTimeMs(long lastModificationTimeMs, boolean override) {
    synchronized (this) {
      if (override || mLastModificationTimeMs < lastModificationTimeMs) {
        mLastModificationTimeMs = lastModificationTimeMs;
      }
      return getThis();
    }
  }

  /**
   * @param lastAccessTimeMs the last access time to use
   * @return the updated object
   */
  public T setLastAccessTimeMs(long lastAccessTimeMs) {
    return setLastAccessTimeMs(lastAccessTimeMs, false);
  }

  /**
   * @param lastAccessTimeMs the last access time to use
   * @param override if true, sets the value regardless of the previous last access time,
   *                 should be set to true for journal replay
   * @return the updated object
   */
  public T setLastAccessTimeMs(long lastAccessTimeMs, boolean override) {
    synchronized (this) {
      if (override || mLastAccessTimeMs < lastAccessTimeMs) {
        mLastAccessTimeMs = lastAccessTimeMs;
      }
      return getThis();
    }
  }

  /**
   * @param name the name to use
   * @return the updated object
   */
  public T setName(String name) {
    mName = name;
    return getThis();
  }

  /**
   * @param parentId the parent id to use
   * @return the updated object
   */
  public T setParentId(long parentId) {
    mParentId = parentId;
    return getThis();
  }

  /**
   * @param ttl the TTL to use, in milliseconds
   * @return the updated object
   */
  public T setTtl(long ttl) {
    mTtl = ttl;
    return getThis();
  }

  /**
   * @param ttlAction the {@link TtlAction} to use
   * @return the updated options object
   */
  public T setTtlAction(TtlAction ttlAction) {
    mTtlAction = ttlAction;
    return getThis();
  }

  /**
   * @param persistenceState the {@link PersistenceState} to use
   * @return the updated object
   */
  public T setPersistenceState(PersistenceState persistenceState) {
    mPersistenceState = persistenceState;
    return getThis();
  }

  /**
   * @param pinned the pinned flag value to use
   * @return the updated object
   */
  public T setPinned(boolean pinned) {
    mPinned = pinned;
    return getThis();
  }

  /**
   * @param owner the owner name of the inode
   * @return the updated object
   */
  public T setOwner(String owner) {
    mAcl.setOwningUser(owner);
    if (isDirectory()) {
      getDefaultACL().setOwningUser(owner);
    }
    return getThis();
  }

  /**
   * @param mode the mode of the inode
   * @return the updated object
   */
  public T setMode(short mode) {
    mAcl.setMode(mode);
    return getThis();
  }

  /**
   * @param acl set the default ACL associated with this inode
   * @throws UnsupportedOperationException if the inode is a file
   * @return the updated object
   */
  public abstract T setDefaultACL(DefaultAccessControlList acl)
      throws UnsupportedOperationException;

  /**
   * Sets ACL entries into the internal ACL.
   * The entries will overwrite any existing correspondent entries in the internal ACL.
   *
   * @param entries the ACL entries
   * @return the updated object
   */
  public T setAcl(List<AclEntry> entries) {
    if (entries == null || entries.isEmpty()) {
      return getThis();
    }
    for (AclEntry entry : entries) {
      if (entry.isDefault()) {
        getDefaultACL().setEntry(entry);
      } else {
        mAcl.setEntry(entry);
      }
    }
    updateMask(entries);
    return getThis();
  }

  /**
   * Sets the internal ACL to a specified ACL.
   * @param acl the specified ACL
   * @return the updated object
   */
  public T setInternalAcl(AccessControlList acl) {
    mAcl = acl;
    return getThis();
  }

  /**
   * @param ufsFingerprint the ufs fingerprint to use
   * @return the updated object
   */
  public T setUfsFingerprint(String ufsFingerprint) {
    mUfsFingerprint = ufsFingerprint;
    return getThis();
  }

  /**
   * @param xAttr The new set of extended attributes
   * @return the updated object
   */
  public T setXAttr(Map<String, byte[]> xAttr) {
    mXAttr = xAttr;
    return getThis();
  }

  /**
   * @param mediumTypes the medium types to pin to
   * @return the updated object
   */
  public T setMediumTypes(Set<String> mediumTypes) {
    mMediumTypes = mediumTypes;
    return getThis();
  }

  @Override
  public abstract FileInfo generateClientFileInfo(String path);

  /**
   * @return {@code this} so that the abstract class can use the fluent builder pattern
   */
  protected abstract T getThis();

  @Override
  public boolean checkPermission(String user, List<String> groups, AclAction action) {
    return mAcl.checkPermission(user, groups, action);
  }

  @Override
  public AclActions getPermission(String user, List<String> groups) {
    return mAcl.getPermission(user, groups);
  }

  /**
   * Casts the inode as an {@link InodeDirectory} if it is one, otherwise throws an
   * exception.
   *
   * This gives convenience in method chaining, e.g.
   *
   * inode.asDirectory().getChildren()
   *
   * instead of
   *
   * ((MutableInodeDirectory) inode).getChildren()
   *
   * @return the inode as an inode directory
   */
  public MutableInodeDirectory asDirectory() {
    if (!isDirectory()) {
      throw new IllegalStateException(String.format("Inode %s is not a directory", getName()));
    }
    return (MutableInodeDirectory) this;
  }

  /**
   * @return the inode as an inode file
   */
  public MutableInodeFile asFile() {
    if (isDirectory()) {
      throw new IllegalStateException(String.format("Inode %s is not a file", getName()));
    }
    return (MutableInodeFile) this;
  }

  /**
   * Updates this inode's state from the given entry.
   *
   * @param entry the entry
   */
  public void updateFromEntry(UpdateInodeEntry entry) {
    if (entry.hasAcl()) {
      setInternalAcl(ProtoUtils.fromProto(entry.getAcl()));
    }
    if (entry.hasCreationTimeMs()) {
      setCreationTimeMs(entry.getCreationTimeMs());
    }
    if (entry.hasGroup() && !entry.getGroup().isEmpty()) {
      setGroup(entry.getGroup());
    }
    if (entry.hasLastModificationTimeMs()) {
      setLastModificationTimeMs(entry.getLastModificationTimeMs(),
          entry.getOverwriteModificationTime());
    }
    if (entry.hasLastAccessTimeMs()) {
      setLastAccessTimeMs(entry.getLastAccessTimeMs(),
          entry.getOverwriteAccessTime());
    }
    if (entry.hasMode()) {
      setMode((short) entry.getMode());
    }
    if (entry.getMediumTypeCount() != 0) {
      setMediumTypes(new HashSet<>(entry.getMediumTypeList()));
    }
    if (entry.hasName()) {
      setName(entry.getName());
    }
    if (entry.hasOwner() && !entry.getOwner().isEmpty()) {
      setOwner(entry.getOwner());
    }
    if (entry.hasParentId()) {
      setParentId(entry.getParentId());
    }
    if (entry.hasPersistenceState()) {
      setPersistenceState(PersistenceState.valueOf(entry.getPersistenceState()));
    }
    if (entry.hasPinned()) {
      setPinned(entry.getPinned());
    }
    if (entry.hasTtl()) {
      setTtl(entry.getTtl());
    }
    if (entry.hasTtlAction()) {
      setTtlAction(ProtobufUtils.fromProtobuf(entry.getTtlAction()));
    }
    if (entry.hasUfsFingerprint()) {
      setUfsFingerprint(entry.getUfsFingerprint());
    }
    if (entry.getXAttrCount() > 0) {
      setXAttr(CommonUtils.convertFromByteString(entry.getXAttrMap()));
    }
    if (entry.hasPinned()) {
      // pinning status has changed, therefore we change the medium list with it.
      if (entry.getPinned()) {
        List<String> mediaList = ServerConfiguration.getList(
            PropertyKey.MASTER_TIERED_STORE_GLOBAL_MEDIUMTYPE, ",");
        setMediumTypes(entry.getMediumTypeList().stream()
            .filter(mediaList::contains).collect(Collectors.toSet()));
      } else {
        setMediumTypes(Collections.emptySet());
      }
    }
  }

  @Override
  public int hashCode() {
    return ((Long) mId).hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MutableInode<?>)) {
      return false;
    }
    MutableInode<?> that = (MutableInode<?>) o;
    return mId == that.mId;
  }

  /**
   * @param path path of the inode
   * @return the journal entry representing the inode
   */
  public abstract Journal.JournalEntry toJournalEntry(String path);

  protected MoreObjects.ToStringHelper toStringHelper() {
    return MoreObjects.toStringHelper(this)
        .add("id", mId)
        .add("name", mName)
        .add("parentId", mParentId)
        .add("creationTimeMs", mCreationTimeMs)
        .add("pinned", mPinned)
        .add("deleted", mDeleted)
        .add("ttl", mTtl)
        .add("ttlAction", mTtlAction)
        .add("directory", mDirectory)
        .add("persistenceState", mPersistenceState)
        .add("lastModificationTimeMs", mLastModificationTimeMs)
        .add("lastAccessTimeMs", mLastAccessTimeMs)
        .add("owner", mAcl.getOwningUser())
        .add("group", mAcl.getOwningGroup())
        .add("permission", mAcl.getMode())
        .add("ufsFingerprint", mUfsFingerprint)
        .add("mediatypes", mMediumTypes)
        .add("xAttr", mXAttr);
  }

  protected InodeMeta.Inode.Builder toProtoBuilder() {
    InodeMeta.Inode.Builder inode = InodeMeta.Inode.newBuilder()
        .setId(getId())
        .setCreationTimeMs(getCreationTimeMs())
        .setIsDirectory(isDirectory())
        .setTtl(getTtl())
        .setTtlAction(getTtlAction())
        .setLastModifiedMs(getLastModificationTimeMs())
        .setLastAccessedMs(getLastAccessTimeMs())
        .setName(getName())
        .setParentId(getParentId())
        .setPersistenceState(getPersistenceState().name())
        .setIsPinned(isPinned())
        .setAccessAcl(ProtoUtils.toProto(getACL()))
        .setUfsFingerprint(getUfsFingerprint())
        .addAllMediumType(getMediumTypes());
    if (getXAttr() != null) {
      inode.putAllXAttr(CommonUtils.convertToByteString(getXAttr()));
    }
    return inode;
  }

  /**
   * @param inode a protocol buffer inode
   * @return the {@link MutableInode} representation for the inode
   */
  public static MutableInode<?> fromProto(InodeOrBuilder inode) {
    if (inode.getIsDirectory()) {
      return MutableInodeDirectory.fromProto(inode);
    } else {
      return MutableInodeFile.fromProto(inode);
    }
  }
}
