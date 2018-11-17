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

import alluxio.grpc.RenamePOptions;
import alluxio.master.file.FileSystemMasterOptions;

/**
 * Wrapper for {@link RenamePOptions} with additional context data.
 */
public class RenameContext extends OperationContext<RenamePOptions.Builder> {

    private long mOperationTimeMs;

    // Prevent instantiation
    private RenameContext(){super(null);};

    /**
     * Merges and embeds the given {@link RenamePOptions} with the corresponding master options.
     * @param options Proto {@link RenamePOptions} to embed
     * @return the instance of {@link RenameContext} with default values for master
     */
    public static RenameContext defaults(RenamePOptions options){
        RenamePOptions masterOptions = FileSystemMasterOptions.getRenameOptions();
        RenamePOptions mergedOptions = masterOptions.toBuilder().mergeFrom(options).build();
        return new RenameContext(mergedOptions);
    }

    /**
     * @return the instance of {@link RenameContext} with default values for master
     */
    public static RenameContext defaults() {
        RenamePOptions masterOptions = FileSystemMasterOptions.getRenameOptions();
        return new RenameContext(masterOptions);
    }

    /**
     * Creates rename context with given option data.
     * @param options rename options
     */
    private RenameContext(RenamePOptions options) {
        super(options.toBuilder());
        mOperationTimeMs = System.currentTimeMillis();
    }

    /**
     * Sets operation time.
     * @param operationTimeMs operation system time in ms
     * @return the updated context instance
     */
    public RenameContext setOperationTimeMs(long operationTimeMs) {
        mOperationTimeMs = operationTimeMs;
        return this;
    }

    /**
     * @return the operation system time in ms
     */
    public long getOperationTimeMs() {
        return mOperationTimeMs;
    }
}
