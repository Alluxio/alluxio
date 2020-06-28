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

package alluxio.master.file.contexts;

import alluxio.client.WriteType;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.TtlAction;
import alluxio.grpc.WritePType;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.Mode;
import alluxio.util.SecurityUtils;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.GeneratedMessageV3;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * Wrapper for {@link CreateFilePOptions} or {@link CreateDirectoryPOptions} with additional context
 * data.
 *
 * @param <T> Proto Builder type
 * @param <K> Path context type
 */
public abstract class CreatePathContext<T extends GeneratedMessageV3.Builder<?>,
    K extends CreatePathContext<?, ?>> extends OperationContext<T, K> {

  protected boolean mMountPoint;
  protected long mOperationTimeMs;
  protected List<AclEntry> mAcl;
  protected String mOwner;
  protected String mGroup;
  protected boolean mMetadataLoad;
  private WriteType mWriteType;
  protected Map<String, byte[]> mXAttr;

  //
  // Values for the below fields will be extracted from given proto options
  // No setters are provided for the extracted fields in order to protect consistency
  // between the context and the underlying proto options
  //
  //
  // PS: This extraction can be eliminated by splitting {@link InodeTree.createPath}.
  //
  protected Mode mMode;
  protected boolean mRecursive;
  protected long mTtl;
  protected TtlAction mTtlAction;

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder the options builder
   */
  protected CreatePathContext(T optionsBuilder) {
    super(optionsBuilder);
    mMountPoint = false;
    mOperationTimeMs = System.currentTimeMillis();
    mAcl = Collections.emptyList();
    mMetadataLoad = false;
    mGroup = "";
    mOwner = "";
    if (SecurityUtils.isAuthenticationEnabled(ServerConfiguration.global())) {
      mOwner = SecurityUtils.getOwnerFromGrpcClient(ServerConfiguration.global());
      mGroup = SecurityUtils.getGroupFromGrpcClient(ServerConfiguration.global());
    }
    // Initialize mPersisted based on proto write type.
    WritePType writeType = WritePType.NONE;
    if (optionsBuilder instanceof CreateFilePOptions.Builder) {
      writeType = ((CreateFilePOptions.Builder) optionsBuilder).getWriteType();
    } else if (optionsBuilder instanceof CreateDirectoryPOptions.Builder) {
      writeType = ((CreateDirectoryPOptions.Builder) optionsBuilder).getWriteType();
    }
    mWriteType = WriteType.fromProto(writeType);
    mXAttr = null;
  }

  private void loadExtractedFields() {
    com.google.protobuf.GeneratedMessageV3.Builder<?> optionsBuilder = getOptions();
    // Populate extracted fields from given options
    if (optionsBuilder instanceof CreateFilePOptions.Builder) {
      CreateFilePOptions.Builder fileOptions = (CreateFilePOptions.Builder) optionsBuilder;
      mMode = Mode.fromProto(fileOptions.getMode());
      mRecursive = fileOptions.getRecursive();
      mTtl = fileOptions.getCommonOptions().getTtl();
      mTtlAction = fileOptions.getCommonOptions().getTtlAction();
    }

    // Populate extracted fields from given options
    if (optionsBuilder instanceof CreateDirectoryPOptions.Builder) {
      CreateDirectoryPOptions.Builder dirOptions = (CreateDirectoryPOptions.Builder) optionsBuilder;
      mMode = Mode.fromProto(dirOptions.getMode());
      mRecursive = dirOptions.getRecursive();
      mTtl = dirOptions.getCommonOptions().getTtl();
      mTtlAction = dirOptions.getCommonOptions().getTtlAction();
    }
  }

  /**
   * @return Mode, extracted from options
   */
  public Mode getMode() {
    loadExtractedFields();
    return mMode;
  }

  /**
   * @return true if persisted, extracted from options
   */
  public boolean isPersisted() {
    loadExtractedFields();
    return mWriteType.isThrough();
  }

  /**
   * @return true if recursive, extracted from options
   */
  public boolean isRecursive() {
    loadExtractedFields();
    return mRecursive;
  }

  /**
   * @return ttl, extracted from options
   */
  public long getTtl() {
    loadExtractedFields();
    return mTtl;
  }

  /**
   * @return ttl action, extracted from options
   */
  public TtlAction getTtlAction() {
    loadExtractedFields();
    return mTtlAction;
  }

  /**
   * Note: This is safe as 'K' extends {@link CreatePathContext}.
   *
   * @return the object's type
   */
  protected K getThis() {
    return (K) this;
  }

  /**
   * @param operationTimeMs the operation time to use
   * @return the updated context
   */
  public K setOperationTimeMs(long operationTimeMs) {
    mOperationTimeMs = operationTimeMs;
    return getThis();
  }

  /**
   * @return the operation time
   */
  public long getOperationTimeMs() {
    return mOperationTimeMs;
  }

  /**
   * Sets an immutable copy of acl as the internal access control list.
   *
   * @param acl the ACL entries
   * @return the updated context
   */
  public K setAcl(List<AclEntry> acl) {
    mAcl = ImmutableList.copyOf(acl);
    return getThis();
  }

  /**
   * @return an immutable list of ACL entries
   */
  public List<AclEntry> getAcl() {
    return mAcl;
  }

  /**
   * @param mountPoint the mount point flag to use; it specifies whether the object to create is a
   *        mount point
   * @return the updated context
   */
  public K setMountPoint(boolean mountPoint) {
    mMountPoint = mountPoint;
    return getThis();
  }

  /**
   * @return the mount point flag; it specifies whether the object to create is a mount point
   */
  public boolean isMountPoint() {
    return mMountPoint;
  }

  /**
   * @param owner the owner to use
   * @return the updated context
   */
  public K setOwner(String owner) {
    mOwner = owner;
    return getThis();
  }

  /**
   * @return the owner
   */
  public String getOwner() {
    return mOwner;
  }

  /**
   * @param group the group to use
   * @return the updated context
   */
  public K setGroup(String group) {
    mGroup = group;
    return getThis();
  }

  /**
   * @return the write type of this path
   */
  public WriteType getWriteType() {
    return mWriteType;
  }

  /**
   * @param writeType type of write on this create
   * @return the updated context
   */
  public K setWriteType(WriteType writeType) {
    mWriteType = writeType;
    return getThis();
  }

  /**
   * @return the group
   */
  public String getGroup() {
    return mGroup;
  }

  /**
   * @param metadataLoad the flag value to use; if true, the create path is a result of a metadata
   *        load
   * @return the updated context
   */
  public K setMetadataLoad(boolean metadataLoad) {
    mMetadataLoad = metadataLoad;
    return getThis();
  }

  /**
   * @return extended attributes on this context
   */
  @Nullable
  public Map<String, byte[]> getXAttr() {
    return mXAttr;
  }

  /**
   * @param xattr extended attributes to set when creating
   * @return the updated context
   */
  public K setXAttr(@Nullable Map<String, byte[]> xattr) {
    mXAttr = xattr;
    return getThis();
  }

  /**
   * @return the metadataLoad flag; if true, the create path is a result of a metadata load
   */
  public boolean isMetadataLoad() {
    return mMetadataLoad;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("ProtoOptions", getOptions().build())
        .add("MountPoint", mMountPoint)
        .add("Acl", mAcl)
        .add("Owner", mOwner)
        .add("Group", mGroup)
        .add("MetadataLoad", mMetadataLoad)
        .add("writeType", mWriteType)
        .add("xattr", mXAttr)
        .toString();
  }
}
