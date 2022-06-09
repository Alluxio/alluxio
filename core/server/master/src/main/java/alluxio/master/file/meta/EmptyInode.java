package alluxio.master.file.meta;

import alluxio.grpc.TtlAction;
import alluxio.proto.journal.Journal;
import alluxio.proto.meta.InodeMeta;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.AclAction;
import alluxio.security.authorization.AclActions;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.wire.FileInfo;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * EmptyInode only supports the set and get of inodeName
 */
public class EmptyInode implements InodeView{

  private String mInodeName;

  EmptyInode(String inodeName) {
    mInodeName = inodeName;
  }

  @Override
  public boolean equals(Object o) {
    // TODO(Jiadong): is it a good equals?
    if(this == o) {
      return true;
    }
    if(!(o instanceof InodeView)) {
      return false;
    }
    InodeView inodeView = (InodeView) o;
    return inodeView.getName().equals(mInodeName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mInodeName);
  }

  @Override
  public Journal.JournalEntry toJournalEntry() {
    throw new UnsupportedOperationException("toJournalEntry is not supported in EmptyInode");
  }

  @Override
  public long getCreationTimeMs() {
    throw new UnsupportedOperationException("getCreationTimeMs is not supported in EmptyInode");
  }

  @Override
  public String getGroup() {
    throw new UnsupportedOperationException("getGroup is not supported in EmptyInode");
  }

  @Override
  public long getId() {
    throw new UnsupportedOperationException("getId is not supported in EmptyInode");
  }

  @Override
  public long getTtl() {
    throw new UnsupportedOperationException("getTtl is not supported in EmptyInode");
  }

  @Override
  public TtlAction getTtlAction() {
    throw new UnsupportedOperationException("getTtlAction is not supported in EmptyInode");
  }

  @Override
  public long getLastModificationTimeMs() {
    throw new
        UnsupportedOperationException("getLastModificationTimeMs is not supported in EmptyInode");
  }

  @Override
  public long getLastAccessTimeMs() {
    throw new UnsupportedOperationException("getLastAccessTimeMs is not supported in EmptyInode");
  }

  @Override
  public String getName() {
    return mInodeName;
  }

  @Override
  public short getMode() {
    throw new UnsupportedOperationException("getMode is not supported in EmptyInode");
  }

  @Override
  public PersistenceState getPersistenceState() {
    throw new UnsupportedOperationException("getPersistenceState is not supported in EmptyInode");
  }

  @Override
  public long getParentId() {
    throw new UnsupportedOperationException("getParentId is not supported in EmptyInode");
  }

  @Override
  public String getOwner() {
    throw new UnsupportedOperationException("getOwner is not supported in EmptyInode");
  }

  @Nullable
  @Override
  public Map<String, byte[]> getXAttr() {
    throw new UnsupportedOperationException("getXAttr is not supported in EmptyInode");
  }

  @Override
  public boolean isDeleted() {
    throw new UnsupportedOperationException("isDeleted is not supported in EmptyInode");
  }

  @Override
  public boolean isDirectory() {
    throw new UnsupportedOperationException("isDirectory is not supported in EmptyInode");
  }

  @Override
  public boolean isFile() {
    throw new UnsupportedOperationException("isFile is not supported in EmptyInode");
  }

  @Override
  public boolean isPinned() {
    throw new UnsupportedOperationException("isPinned is not supported in EmptyInode");
  }

  @Override
  public boolean isPersisted() {
    throw new UnsupportedOperationException("isPersisted is not supported in EmptyInode");
  }

  @Override
  public ImmutableSet<String> getMediumTypes() {
    throw new UnsupportedOperationException("getMediumTypes is not supported in EmptyInode");
  }

  @Override
  public String getUfsFingerprint() {
    throw new UnsupportedOperationException("getUfsFingerprint is not supported in EmptyInode");
  }

  @Override
  public AccessControlList getACL() {
    throw new UnsupportedOperationException("getACL is not supported in EmptyInode");
  }

  @Override
  public DefaultAccessControlList getDefaultACL() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("getDefaultACL is not supported in EmptyInode");
  }

  @Override
  public FileInfo generateClientFileInfo(String path) {
    throw new UnsupportedOperationException("generateClientFileInfo is not supported in EmptyInode");
  }

  @Override
  public boolean checkPermission(String user, List<String> groups, AclAction action) {
    throw new UnsupportedOperationException("checkPermission is not supported in EmptyInode");
  }

  @Override
  public AclActions getPermission(String user, List<String> groups) {
    throw new UnsupportedOperationException("getPermission is not supported in EmptyInode");
  }

  @Override
  public InodeMeta.Inode toProto() {
    throw new UnsupportedOperationException("toProto is not supported in EmptyInode");
  }
}
