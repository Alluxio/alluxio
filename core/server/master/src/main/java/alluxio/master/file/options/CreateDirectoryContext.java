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
import alluxio.underfs.UfsStatus;

/**
 * Wrapper for {@link CreateDirectoryPOptions} with additional context data.
 */
public class CreateDirectoryContext
        extends CreatePathContext<CreateDirectoryPOptions.Builder, CreateDirectoryContext> {

    private UfsStatus mUfsStatus;

    // Prevent instantiation
    private CreateDirectoryContext(){super(null);};

    protected CreateDirectoryContext getThis(){
        return this;
    }

  /**
   * Merges and embeds the given {@link CreateDirectoryPOptions} with the corresponding master
   * options.
   * 
   * @param options Proto {@link CreateDirectoryPOptions} to embed
   * @return the instance of {@link CreateDirectoryContext} with default values for master
   */
  public static CreateDirectoryContext defaults(CreateDirectoryPOptions options) {
    CreateDirectoryPOptions masterOptions = FileSystemMasterOptions.getCreateDirectoryOptions();
    CreateDirectoryPOptions mergedOptions = masterOptions.toBuilder().mergeFrom(options).build();
    return new CreateDirectoryContext(mergedOptions);
  }

  /**
   * @return the instance of {@link CreateDirectoryContext} with default values for master
   */
  public static CreateDirectoryContext defaults() {
    CreateDirectoryPOptions masterOptions = FileSystemMasterOptions.getCreateDirectoryOptions();
    return new CreateDirectoryContext(masterOptions);
  }

    /**
     * Creates rename context with given option data.
     * @param options rename options
     */
    private CreateDirectoryContext(CreateDirectoryPOptions options) {
        super(options.toBuilder());
        mUfsStatus = null;
    }

    /**
     * Sets {@link UfsStatus} for the directory
     * @param ufsStatus Ufs status to set
     * @return the updated context instance
     */
    public CreateDirectoryContext setUfsStatus(UfsStatus ufsStatus) {
        mUfsStatus = ufsStatus;
        return getThis();
    }

    /**
     * @return the Ufs status
     */
    public UfsStatus getUfsStatus() {
        return mUfsStatus;
    }
}