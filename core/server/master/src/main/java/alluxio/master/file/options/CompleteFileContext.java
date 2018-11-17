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

import alluxio.grpc.CompleteFilePOptions;
import alluxio.master.file.FileSystemMasterOptions;
import alluxio.underfs.UfsStatus;

/**
 * Wrapper for {@link CompleteFilePOptions} with additional context data.
 */
public class CompleteFileContext extends OperationContext<CompleteFilePOptions.Builder> {

    private long mOperationTimeMs;
    private UfsStatus mUfsStatus;

    // Prevent instantiation
    private CompleteFileContext(){super(null);};

    /**
     * Merges and embeds the given {@link CompleteFilePOptions} with the corresponding master options.
     * @param options Proto {@link CompleteFilePOptions} to embed
     * @return the instance of {@link CompleteFileContext} with default values for master
     */
    public static CompleteFileContext defaults(CompleteFilePOptions options){
        CompleteFilePOptions masterOptions = FileSystemMasterOptions.getCompleteFileOptions();
        CompleteFilePOptions mergedOptions = masterOptions.toBuilder().mergeFrom(options).build();
        return new CompleteFileContext(mergedOptions);
    }

    /**
     * @return the instance of {@link CompleteFileContext} with default values for master
     */
    public static CompleteFileContext defaults() {
        CompleteFilePOptions masterOptions = FileSystemMasterOptions.getCompleteFileOptions();
        return new CompleteFileContext(masterOptions);
    }

    /**
     * Creates rename context with given option data.
     * @param options rename options
     */
    private CompleteFileContext(CompleteFilePOptions options) {
        super(options.toBuilder());
        mOperationTimeMs = System.currentTimeMillis();
        mUfsStatus = null;
    }

    /**
     * Sets operation time.
     * @param operationTimeMs operation system time in ms
     * @return the updated context instance
     */
    public CompleteFileContext setOperationTimeMs(long operationTimeMs) {
        mOperationTimeMs = operationTimeMs;
        return this;
    }

    /**
     * @return the ufs status
     */
    public UfsStatus getUfsStatus() {
        return mUfsStatus;
    }

    /**
     * Sets ufs status.
     * @param ufsStatus ufs status
     * @return the updated context instance
     */
    public CompleteFileContext setUfsStatus(UfsStatus ufsStatus) {
        mUfsStatus = ufsStatus;
        return this;
    }

    /**
     * @return the operation system time in ms
     */
    public long getOperationTimeMs() {
        return mOperationTimeMs;
    }
}
