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

import alluxio.conf.ServerConfiguration;
import alluxio.grpc.ScheduleAsyncPersistencePOptions;
import alluxio.util.FileSystemOptions;

import com.google.common.base.MoreObjects;

/**
 * Used to merge and wrap {@link ScheduleAsyncPersistencePOptions}.
 */
public class ScheduleAsyncPersistenceContext
    extends
    OperationContext<ScheduleAsyncPersistencePOptions.Builder, ScheduleAsyncPersistenceContext> {

  private long mPersistenceWaitTime;

  /**
   * Creates context with given option data.
   *
   * @param optionsBuilder options builder
   */
  private ScheduleAsyncPersistenceContext(ScheduleAsyncPersistencePOptions.Builder optionsBuilder) {
    super(optionsBuilder);
    mPersistenceWaitTime = optionsBuilder.getPersistenceWaitTime();
  }

  /**
   * @param optionsBuilder Builder for proto {@link ScheduleAsyncPersistencePOptions}
   * @return the instance of {@link ScheduleAsyncPersistenceContext} with the given options
   */
  public static ScheduleAsyncPersistenceContext create(
      ScheduleAsyncPersistencePOptions.Builder optionsBuilder) {
    return new ScheduleAsyncPersistenceContext(optionsBuilder);
  }

  /**
   * Merges and embeds the given {@link ScheduleAsyncPersistencePOptions}
   * with the corresponding master options.
   *
   * @param optionsBuilder Builder for proto {@link ScheduleAsyncPersistencePOptions} to embed
   * @return the instance of {@link ScheduleAsyncPersistenceContext} with default values for master
   */
  public static ScheduleAsyncPersistenceContext mergeFrom(
      ScheduleAsyncPersistencePOptions.Builder optionsBuilder) {
    ScheduleAsyncPersistencePOptions masterOptions =
        FileSystemOptions.scheduleAsyncPersistenceDefaults(ServerConfiguration.global());
    ScheduleAsyncPersistencePOptions.Builder mergedOptionsBuilder =
        masterOptions.toBuilder().mergeFrom(optionsBuilder.build());
    return create(mergedOptionsBuilder);
  }

  /**
   * @return the instance of {@link LoadMetadataContext} with default values for master
   */
  public static ScheduleAsyncPersistenceContext defaults() {
    return create(FileSystemOptions
        .scheduleAsyncPersistenceDefaults(ServerConfiguration.global()).toBuilder());
  }

  /**
   * @return the persistence wait time
   */
  public long getPersistenceWaitTime() {
    return mPersistenceWaitTime;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("ProtoOptions", getOptions().build())
        .add("persistenceWaitTime", mPersistenceWaitTime)
        .toString();
  }
}
