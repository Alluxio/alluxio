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
import alluxio.grpc.FreeWorkerPOptions;
import alluxio.util.FileSystemOptions;

import alluxio.grpc.FreeWorkerPOptions;

import com.google.common.base.MoreObjects;

public class FreeWorkerContext extends OperationContext<FreeWorkerPOptions.Builder, FreeWorkerContext>{

    /**
     * Creates context with given option data.
     * @param optionsBuilder options builder
     */
    private FreeWorkerContext(FreeWorkerPOptions.Builder optionsBuilder) {
        super(optionsBuilder);
    }

    /**
     * @param optionsBuilder Builder for proto {@link FreeWorkerPOptions}
     * @return the instance of {@link FreeWorkerContext} with the given options
     */
    public static FreeWorkerContext create(FreeWorkerPOptions.Builder optionsBuilder) {
        return new FreeWorkerContext(optionsBuilder);
    }

    /**
     * Merges and embeds the given {@link FreeWorkerPOptions} with the corresponding master options.
     * *
     * @param optionsBuilder Builder for proto {@link FreeWorkerPOptions} to merge with defaults
     * @return the instance of {@link FreeWorkerContext} with default values for master
     */
    public static FreeWorkerContext mergeFrom(FreeWorkerPOptions.Builder optionsBuilder) {
        FreeWorkerPOptions masterOptions = FileSystemOptions.freeWorkerDefaults(Configuration.global());
        FreeWorkerPOptions.Builder mergedOptionsBuilder =
                masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
        return create(mergedOptionsBuilder);
    }

    /**
     * @return the instance of {@link FreeWorkerContext} with default values for master
     */
    public static FreeWorkerContext defaults() {
        return create(FileSystemOptions.freeWorkerDefaults(Configuration.global()).toBuilder());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("ProtoOptions", getOptions().build())
                .toString();
    }
}
