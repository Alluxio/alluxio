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

package alluxio.client.file.options;

import alluxio.client.ReadType;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.policy.options.CreateOptions;
import alluxio.client.file.FileSystemClientOptions;
import alluxio.client.file.URIStatus;
import alluxio.grpc.OpenFilePOptions;
import alluxio.master.block.BlockId;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.grpc.GrpcUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.FileBlockInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Information for reading a file. This is an internal options class which contains information
 * from {@link OpenFilePOptions} as well as the {@link URIStatus} being read. In addition to
 * providing access to the fields, it provides convenience functions for various nested
 * fields and creating {@link alluxio.proto.dataserver.Protocol.ReadRequest}s.
 */
@NotThreadSafe
// TODO(calvin): Rename this class
public final class InStreamOptions {
  private final URIStatus mStatus;
  private final OpenFilePOptions mProtoOptions;
  private BlockLocationPolicy mUfsReadLocationPolicy;

  /**
   * Creates with the default {@link OpenFilePOptions}.
   * @param status the file to create the options for
   */
  public InStreamOptions(URIStatus status) {
    this(status, FileSystemClientOptions.getOpenFileOptions());
  }

  /**
   * Creates with given {@link OpenFilePOptions} instance.
   * @param status URI status
   * @param options OpenFile options
   */
  public InStreamOptions(URIStatus status, OpenFilePOptions options) {
    mStatus = status;
    mProtoOptions = options;
    CreateOptions blockLocationPolicyCreateOptions = CreateOptions.defaults()
            .setLocationPolicyClassName(options.getFileReadLocationPolicy())
            .setDeterministicHashPolicyNumShards(options.getHashingNumberOfShards());
    mUfsReadLocationPolicy = BlockLocationPolicy.Factory.create(blockLocationPolicyCreateOptions);
  }

  /**
   * @return the {@link OpenFilePOptions} associated with the instream
   */
  public OpenFilePOptions getOptions() {
    return mProtoOptions;
  }

  /**
   * Sets block read location policy.
   *
   * @param ufsReadLocationPolicy block location policy implementation
   */
  @VisibleForTesting
  public void setUfsReadLocationPolicy(BlockLocationPolicy ufsReadLocationPolicy) {

    mUfsReadLocationPolicy = Preconditions.checkNotNull(ufsReadLocationPolicy);
  }

  /**
   * @return the {@link BlockLocationPolicy} associated with the instream
   */
  public BlockLocationPolicy getUfsReadLocationPolicy() {
    return mUfsReadLocationPolicy;
  }

  /**
   * @return the {@link URIStatus} associated with the instream
   */
  public URIStatus getStatus() {
    return mStatus;
  }

  /**
   * @param blockId id of the block
   * @return the block info associated with the block id, note that this will be a cached copy
   * and will not fetch the latest info from the master
   */
  public BlockInfo getBlockInfo(long blockId) {
    Preconditions.checkArgument(mStatus.getBlockIds().contains(blockId), "blockId");
    return mStatus.getFileBlockInfos().stream().map(FileBlockInfo::getBlockInfo)
        .filter(blockInfo -> blockInfo.getBlockId() == blockId).findFirst().get();
  }

  /**
   * @param blockId id of the block
   * @return a {@link Protocol.OpenUfsBlockOptions} based on the block id and options
   */
  public Protocol.OpenUfsBlockOptions getOpenUfsBlockOptions(long blockId) {
    Preconditions.checkArgument(mStatus.getBlockIds().contains(blockId), "blockId");
    boolean readFromUfs = mStatus.isPersisted();
    // In case it is possible to fallback to read UFS blocks, also fill in the options.
    boolean storedAsUfsBlock = mStatus.getPersistenceState().equals("TO_BE_PERSISTED");
    readFromUfs = readFromUfs || storedAsUfsBlock;
    if (!readFromUfs) {
      return Protocol.OpenUfsBlockOptions.getDefaultInstance();
    }
    long blockStart = BlockId.getSequenceNumber(blockId) * mStatus.getBlockSizeBytes();
    BlockInfo info = getBlockInfo(blockId);
    Protocol.OpenUfsBlockOptions openUfsBlockOptions = Protocol.OpenUfsBlockOptions.newBuilder()
        .setUfsPath(mStatus.getUfsPath()).setOffsetInFile(blockStart).setBlockSize(info.getLength())
        .setMaxUfsReadConcurrency(mProtoOptions.getMaxUfsReadConcurrency())
        .setNoCache(!ReadType.fromProto(mProtoOptions.getReadType()).isCache())
        .setMountId(mStatus.getMountId()).build();
    if (storedAsUfsBlock) {
      // On client-side, we do not have enough mount information to fill in the UFS file path.
      // Instead, we unset the ufsPath field and fill in a flag ufsBlock to indicate the UFS file
      // path can be derived from mount id and the block ID. Also because the entire file is only
      // one block, we set the offset in file to be zero.
      openUfsBlockOptions = openUfsBlockOptions.toBuilder().clearUfsPath().setBlockInUfsTier(true)
            .setOffsetInFile(0).build();
    }
    return openUfsBlockOptions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof InStreamOptions)) {
      return false;
    }
    InStreamOptions that = (InStreamOptions) o;
    return Objects.equal(mStatus, that.mStatus)
        && Objects.equal(mProtoOptions, that.mProtoOptions);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        mStatus,
        mProtoOptions
    );
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("URIStatus", mStatus)
        .add("OpenFileOptions", mProtoOptions)
        .toString();
  }
}
