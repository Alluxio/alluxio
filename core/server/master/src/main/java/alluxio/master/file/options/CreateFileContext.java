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

import alluxio.grpc.CreateFilePOptions;
import alluxio.master.file.FileSystemMasterOptions;

/**
 * Wrapper for {@link CreateFilePOptions} with additional context data.
 */
public class CreateFileContext
    extends CreatePathContext<CreateFilePOptions.Builder, CreateFileContext> {

    private boolean mCacheable;

    // Prevent instantiation
    private CreateFileContext(){super(null);};

    protected CreateFileContext getThis(){
        return this;
    }

    /**
     * Merges and embeds the given {@link CreateFilePOptions} with the corresponding master options.
     * @param options Proto {@link CreateFilePOptions} to embed
     * @return the instance of {@link CreateFileContext} with default values for master
     */
    public static CreateFileContext defaults(CreateFilePOptions options){
        CreateFilePOptions masterOptions = FileSystemMasterOptions.getCreateFileOptions();
        CreateFilePOptions mergedOptions = masterOptions.toBuilder().mergeFrom(options).build();
        return new CreateFileContext(mergedOptions);
    }

    /**
     * @return the instance of {@link CreateFileContext} with default values for master
     */
    public static CreateFileContext defaults() {
        CreateFilePOptions masterOptions = FileSystemMasterOptions.getCreateFileOptions();
        return new CreateFileContext(masterOptions);
    }

    /**
     * Creates context with given option data.
     * @param options options
     */
    private CreateFileContext(CreateFilePOptions options) {
        super(options.toBuilder());
        mCacheable = false;
    }

    /**
     * @param cacheable true if the file is cacheable, false otherwise
     * @return the updated context object
     */
    public CreateFileContext setCacheable(boolean cacheable) {
        mCacheable = cacheable;
        return this;
    }

    /**
     * @return true if file is cacheable
     */
    public boolean isCacheable() {
        return mCacheable;
    }
}
