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

package alluxio.file.options;

import alluxio.grpc.RenamePOptions;

/**
 * Wrapper for {@link RenamePOptions} with additional context data.
 */
public class RenameContext extends OperationContext<RenamePOptions>{

    private long mOperationTimeMs;

    /**
     * Creates rename context with given option data.
     * @param options rename options
     */
    public RenameContext(RenamePOptions options) {
        super(options);
        mOperationTimeMs = System.currentTimeMillis();
    }

    /**
     * Sets operation time.
     * @param operationTimeMs operation system time in ms
     * @return the context instance
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
