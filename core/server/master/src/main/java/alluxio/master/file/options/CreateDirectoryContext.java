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

package alluxio.master.file.options;

import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.master.file.FileSystemMasterOptions;
import alluxio.security.authorization.AclEntry;
import alluxio.underfs.UfsStatus;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Used to merge and wrap {@link CreateDirectoryPOptions}.
 */
public class CreateDirectoryContext
    extends CreatePathContext<CreateDirectoryPOptions.Builder, CreateDirectoryContext> {

  private UfsStatus mUfsStatus;
  protected List<AclEntry> mDefaultAcl;

  // Prevent instantiation
  private CreateDirectoryContext() {
    super(null);
  }

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private CreateDirectoryContext(CreateDirectoryPOptions.Builder optionsBuilder) {
    super(optionsBuilder);
    mUfsStatus = null;
  }

  /**
   * Merges and embeds the given {@link CreateDirectoryPOptions} with the corresponding master
   * options.
   *
   * @param optionsBuilder Builder for proto {@link CreateDirectoryPOptions} to merge with defaults
   * @return the instance of {@link CreateDirectoryContext} with default values for master
   */
  public static CreateDirectoryContext defaults(CreateDirectoryPOptions.Builder optionsBuilder) {
    CreateDirectoryPOptions masterOptions = FileSystemMasterOptions.createDirectoryDefaults();
    CreateDirectoryPOptions.Builder mergedOptionsBuilder =
        masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return new CreateDirectoryContext(mergedOptionsBuilder);
  }

  /**
   * @return the instance of {@link CreateDirectoryContext} with default values for master
   */
  public static CreateDirectoryContext defaults() {
    CreateDirectoryPOptions masterOptions = FileSystemMasterOptions.createDirectoryDefaults();
    return new CreateDirectoryContext(masterOptions.toBuilder());
  }

  protected CreateDirectoryContext getThis() {
    return this;
  }

  /**
   * @return the Ufs status
   */
  public UfsStatus getUfsStatus() {
    return mUfsStatus;
  }

  /**
   * Sets {@link UfsStatus} for the directory.
   *
   * @param ufsStatus Ufs status to set
   * @return the updated context instance
   */
  public CreateDirectoryContext setUfsStatus(UfsStatus ufsStatus) {
    mUfsStatus = ufsStatus;
    return getThis();
  }

  /**
   * Sets the default ACL in the context.
   * @param defaultAcl a list of default ACL Entries
   * @return the updated context object
   */
  public CreateDirectoryContext setDefaultAcl(List<AclEntry> defaultAcl) {
    mDefaultAcl = ImmutableList.copyOf(defaultAcl);
    return getThis();
  }

  /**
   * @return the default ACL in the form of a list of default ACL Entries
   */
  public List<AclEntry> getDefaultAcl() {
    return mDefaultAcl;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("PathContext", super.toString())
        .add("UfsStatus", mUfsStatus.toString())
        .toString();
  }
}
