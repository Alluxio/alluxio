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

import com.google.protobuf.AbstractMessage;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Used as a base class for wrapping context around proto messages.
 *
 * @param <T> Proto message type
 */
@NotThreadSafe
public class OperationContext<T extends AbstractMessage> {
    // Proto message that is being wrapped
    private T mOptions;

    /**
     * Creates an instance with given proto message
     *
     * @param options Internal proto message instance
     */
    public OperationContext(T options) {
        mOptions = options;
    }

    /**
     * @return underlying proto message instance
     */
    public T getOptions() {
        return mOptions;
    }
}
