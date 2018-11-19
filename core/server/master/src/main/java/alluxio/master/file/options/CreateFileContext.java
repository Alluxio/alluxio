/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file.options;

import alluxio.grpc.CreateFilePOptions;
import alluxio.master.file.FileSystemMasterOptions;
import com.google.common.base.MoreObjects;

/**
 * Wrapper for {@link CreateFilePOptions} with additional context data.
 */
public class CreateFileContext
    extends CreatePathContext<CreateFilePOptions.Builder, CreateFileContext> {

  private boolean mCacheable;

  // Prevent instantiation
  private CreateFileContext() {
    super(null);
  };

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private CreateFileContext(CreateFilePOptions.Builder optionsBuilder) {
    super(optionsBuilder);
    mCacheable = false;
  }

  /**
   * Merges and embeds the given {@link CreateFilePOptions} with the corresponding master options.
   * 
   * @param optionsBuilder Builder for proto {@link CreateFilePOptions} to embed
   * @return the instance of {@link CreateFileContext} with default values for master
   */
  public static CreateFileContext defaults(CreateFilePOptions.Builder optionsBuilder) {
    CreateFilePOptions masterOptions = FileSystemMasterOptions.getCreateFileOptions();
    CreateFilePOptions.Builder mergedOptionsBuilder =
        masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return new CreateFileContext(mergedOptionsBuilder);
  }

  /**
   * @return the instance of {@link CreateFileContext} with default values for master
   */
  public static CreateFileContext defaults() {
    CreateFilePOptions masterOptions = FileSystemMasterOptions.getCreateFileOptions();
    return new CreateFileContext(masterOptions.toBuilder());
  }

  protected CreateFileContext getThis() {
    return this;
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
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("PathContext", super.toString())
        .add("Cacheable", mCacheable)
        .toString();
  }
}
