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

  private boolean mCacheable;

  private boolean mIsCompleted;
  private long mLength;
  @Nullable private List<Long> mBlockIds;

  /**
   * If set, the new file will use this id instead of a generated one when the file is created.
   */
  @Nullable private Long mBlockContainerId;

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private CreateFileContext(CreateFilePOptions.Builder optionsBuilder) {
    super(optionsBuilder);
    mCacheable = false;
    mIsCompleted = false;
    mLength = 0;
    mBlockIds = null;
    mBlockContainerId = null;
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
   * @param isCompleted true if the file is completed, otherwise false
   * @return the updated context object
   */
  public CreateFileContext setIsCompleted(boolean isCompleted) {
    mIsCompleted = isCompleted;
    return getThis();
  }

  /**
   * @return true if the file is completed, otherwise false
   */
  public boolean isCompleted() {
    return mIsCompleted;
  }

  /**
   * @param length the file length
   * @return the updated context object
   */
  public CreateFileContext setLength(long length) {
    mLength = length;
    return getThis();
  }

  /**
   * @return the file length
   */
  public long getLength() {
    return mLength;
  }

  /**
   * @param blockIds the block ids of the file. Null if the file is incomplete
   * @return the updated context object
   */
  public CreateFileContext setBlockIds(List<Long> blockIds) {
    mBlockIds = blockIds;
    return this;
  }

  /**
   * @return the block ids of the file. Null if the file is incomplete
   */
  @Nullable
  public List<Long> getBlockIds() {
    return mBlockIds;
  }

  /**
   * @param blockContainerId the block container id
   * @return the updated context object
   */
  public CreateFileContext setBlockContainerId(long blockContainerId) {
    mBlockContainerId = blockContainerId;
    return this;
  }

  /**
   * @return the block container id, null if auto generated one is used
   */
  @Nullable
  public Long getBlockContainerId() {
    return mBlockContainerId;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("PathContext", super.toString())
        .add("Cacheable", mCacheable)
        .add("Length", mLength)
        .add("IsCompleted", mIsCompleted)
        .add("BlockContainerId", mBlockContainerId)
        .toString();
  }
}
