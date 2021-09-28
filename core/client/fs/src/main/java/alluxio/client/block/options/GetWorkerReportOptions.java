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

package alluxio.client.block.options;

import alluxio.grpc.GetWorkerReportPOptions;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Worker information options.
 */
@NotThreadSafe
public final class GetWorkerReportOptions implements Serializable {
  private static final long serialVersionUID = -7604526631057562523L;

  private Set<String> mAddresses;
  private Set<WorkerInfoField> mFieldRange;
  private WorkerRange mWorkerRange;

  /**
   * @return the default {@link GetWorkerReportOptions}
   */
  public static GetWorkerReportOptions defaults() {
    return new GetWorkerReportOptions();
  }

  /**
   * Creates a new instance with default values.
   */
  private GetWorkerReportOptions() {
    mAddresses = new HashSet<>();
    mFieldRange = new HashSet<>(Arrays.asList(WorkerInfoField.values()));
    mWorkerRange = WorkerRange.ALL;
  }

  /**
   * Creates a new instance of {@link GetWorkerReportOptions} from a proto representation.
   *
   * @param options the proto representation of a GetWorkerReportOptions
   */
  public GetWorkerReportOptions(alluxio.grpc.GetWorkerReportPOptions options) {
    mAddresses = new HashSet<>(options.getAddressesList());
    mFieldRange = new HashSet<>();
    for (alluxio.grpc.WorkerInfoField field: options.getFieldRangesList()) {
      mFieldRange.add(WorkerInfoField.fromProto(field));
    }
    mWorkerRange = WorkerRange.fromProto(options.getWorkerRange());
  }

  /**
   * @return the client selected worker addresses
   */
  public Set<String> getAddresses() {
    return mAddresses;
  }

  /**
   * @return the field range of worker info
   */
  public Set<WorkerInfoField> getFieldRange() {
    return mFieldRange;
  }

  /**
   * @return the client selected worker range
   */
  public WorkerRange getWorkerRange() {
    return mWorkerRange;
  }

  /**
   * @param addresses the client selected worker addresses
   * @return the updated options object
   */
  public GetWorkerReportOptions setAddresses(Set<String> addresses) {
    mAddresses = addresses;
    return this;
  }

  /**
   * @param fieldRange the field range of worker info
   * @return the updated options object
   */
  public GetWorkerReportOptions setFieldRange(Set<WorkerInfoField> fieldRange) {
    mFieldRange = fieldRange;
    return this;
  }

  /**
   * @param workerRange the client selected worker range
   * @return the updated options object
   */
  public GetWorkerReportOptions setWorkerRange(WorkerRange workerRange) {
    mWorkerRange = workerRange;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GetWorkerReportOptions)) {
      return false;
    }
    GetWorkerReportOptions that = (GetWorkerReportOptions) o;
    return mAddresses.equals(that.mAddresses)
        && mFieldRange.equals(that.mFieldRange)
        && mWorkerRange.equals(that.mWorkerRange);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mAddresses, mFieldRange, mWorkerRange);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("addresses", mAddresses)
        .add("fieldRange", mFieldRange)
        .add("workerRange", mWorkerRange)
        .toString();
  }

  /**
   * @return Proto representation of the options
   */
  public GetWorkerReportPOptions toProto() {
    GetWorkerReportPOptions.Builder optionsBuilder = GetWorkerReportPOptions.newBuilder();
    optionsBuilder.addAllAddresses(mAddresses);
    if (mFieldRange != null) {
      Set<alluxio.grpc.WorkerInfoField> protoFieldRange = new HashSet<>();
      for (WorkerInfoField field : mFieldRange) {
        protoFieldRange.add(field.toProto());
      }
      optionsBuilder.addAllFieldRanges(protoFieldRange);
    }
    optionsBuilder.setWorkerRange(mWorkerRange.toProto());
    return optionsBuilder.build();
  }

  /**
   * Enum representing the range of workers that we want to show capacity information for.
   */
  public static enum WorkerRange {
    ALL, // All workers
    LIVE, // Live workers
    LOST, // Lost workers
    SPECIFIED; // Combine with mAddresses to define worker range

    /**
     * @return the proto representation of this worker info filter type
     */
    public alluxio.grpc.WorkerRange toProto() {
      return alluxio.grpc.WorkerRange.valueOf(name());
    }

    /**
     * @param workerRange the proto representation of the worker range to create
     * @return the wire type version of the worker range
     */
    public static WorkerRange fromProto(alluxio.grpc.WorkerRange workerRange) {
      return WorkerRange.valueOf(workerRange.name());
    }
  }

  /**
   * Enum representing the fields of the worker information.
   */
  public static enum WorkerInfoField {
    ADDRESS,
    BLOCK_COUNT,
    WORKER_CAPACITY_BYTES,
    WORKER_CAPACITY_BYTES_ON_TIERS,
    ID,
    LAST_CONTACT_SEC,
    START_TIME_MS,
    STATE,
    WORKER_USED_BYTES,
    WORKER_USED_BYTES_ON_TIERS;

    /**
     * @return the proto representation of this worker info fields
     */
    public alluxio.grpc.WorkerInfoField toProto() {
      return alluxio.grpc.WorkerInfoField.valueOf(name());
    }

    /**
     * @param fieldRange the proto representation of the worker info fields
     * @return the wire type version of the worker info field
     */
    public static WorkerInfoField fromProto(
        alluxio.grpc.WorkerInfoField fieldRange) {
      return WorkerInfoField.valueOf(fieldRange.name());
    }
  }
}
