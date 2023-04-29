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

import alluxio.conf.Configuration;
import alluxio.grpc.CreateFilePOptions;
import alluxio.util.FileSystemOptionsUtils;
import alluxio.wire.OperationId;

import com.google.common.base.MoreObjects;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Implementation of {@link OperationContext} used to merge and wrap {@link CreateFilePOptions}.
 */
public class CreateFileContext
    extends CreatePathContext<CreateFilePOptions.Builder, CreateFileContext> {
  /**
   * A class for complete file info.
   */
  public static class CompleteFileInfo {
    /**
     * Constructs an instance.
     * @param containerId the file container id
     * @param length the file size
     * @param blockIds the block ids in the file
     */
    public CompleteFileInfo(long containerId, long length, List<Long> blockIds) {
      mBlockIds = blockIds;
      mContainerId = containerId;
      mLength = length;
    }

    /**
     * If set, the new file will use this id instead of a generated one when the file is created.
     */
    private final long mContainerId;
    private final long mLength;
    private final List<Long> mBlockIds;

    /**
     * @return the container id
     */
    public long getContainerId() {
      return mContainerId;
    }

    /**
     * @return the file length
     */
    public long getLength() {
      return mLength;
    }

    /**
     * @return the block ids in the file
     */
    public List<Long> getBlockIds() {
      return mBlockIds;
    }
  }

  private boolean mCacheable;

  /**
   * If set, the file will be mark as completed when it gets created in the inode tree.
   * Used in metadata sync.
   */
  @Nullable private CompleteFileInfo mCompleteFileInfo;

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private CreateFileContext(CreateFilePOptions.Builder optionsBuilder) {
    super(optionsBuilder);
    mCacheable = false;
    mCompleteFileInfo = null;
  }

  /**
   * @param optionsBuilder Builder for proto {@link CreateFilePOptions}
   * @return the instance of {@link CreateFileContext} with given options
   */
  public static CreateFileContext create(CreateFilePOptions.Builder optionsBuilder) {
    return new CreateFileContext(optionsBuilder);
  }

  /**
   * Merges and embeds the given {@link CreateFilePOptions} with the corresponding master options.
   *
   * @param optionsBuilder Builder for proto {@link CreateFilePOptions} to embed
   * @return the instance of {@link CreateFileContext} with default values for master
   */
  public static CreateFileContext mergeFrom(CreateFilePOptions.Builder optionsBuilder) {
    CreateFilePOptions masterOptions =
        FileSystemOptionsUtils.createFileDefaults(Configuration.global(), false);
    CreateFilePOptions.Builder mergedOptionsBuilder =
        masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return new CreateFileContext(mergedOptionsBuilder);
  }

  /**
   * Merges and creates a CreateFileContext.
   * @param optionsBuilder the options builder template
   * @return the context
   */
  public static CreateFileContext mergeFromDefault(CreateFilePOptions optionsBuilder) {
    return new CreateFileContext(CreateFilePOptions.newBuilder().mergeFrom(optionsBuilder));
  }

  /**
   * @return the instance of {@link CreateFileContext} with default values for master
   */
  public static CreateFileContext defaults() {
    return create(
        FileSystemOptionsUtils.createFileDefaults(Configuration.global(), false).toBuilder());
  }

  /**
   * @return true if file is cacheable
   */
  public boolean isCacheable() {
    return mCacheable;
  }

  /**
   * @param cacheable true if the file is cacheable, false otherwise
   * @return the updated context object
   */
  public CreateFileContext setCacheable(boolean cacheable) {
    mCacheable = cacheable;
    return this;
  }

  @Override
  public OperationId getOperationId() {
    if (getOptions().hasCommonOptions() && getOptions().getCommonOptions().hasOperationId()) {
      return OperationId.fromFsProto(getOptions().getCommonOptions().getOperationId());
    }
    return super.getOperationId();
  }

  /**
   * @param completeFileInfo if the file is expected to mark as completed when it is created
   * @return the updated context object
   */
  public CreateFileContext setCompleteFileInfo(CompleteFileInfo completeFileInfo) {
    mCompleteFileInfo = completeFileInfo;
    return getThis();
  }

  /**
   * @return the complete file info object
   */
  public CompleteFileInfo getCompleteFileInfo() {
    return mCompleteFileInfo;
  }

  @Override
  public String toString() {
    MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this)
        .add("PathContext", super.toString())
        .add("Cacheable", mCacheable);

    if (mCompleteFileInfo != null) {
      helper.add("Length", mCompleteFileInfo.getLength())
          .add("IsCompleted", true)
          .add("BlockContainerId", mCompleteFileInfo.getContainerId());
    }
    return helper.toString();
  }
}
