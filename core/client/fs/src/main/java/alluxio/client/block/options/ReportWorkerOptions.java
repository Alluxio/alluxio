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

import alluxio.thrift.GetReportWorkerInfoListTOptions;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Report worker options.
 */
@NotThreadSafe
public final class ReportWorkerOptions implements Serializable {
  private static final long serialVersionUID = -7604526631057562523L;

  private Set<String> mAddresses;
  private Set<ReportWorkerInfoField> mFieldRange;
  private WorkerRange mWorkerRange;

  /**
   * @return the default {@link ReportWorkerOptions}
   */
  public static ReportWorkerOptions defaults() {
    return new ReportWorkerOptions();
  }

  /**
   * Creates a new instance with default values.
   */
  private ReportWorkerOptions() {
    mAddresses = null;
    mFieldRange = new HashSet<>(Arrays.asList(ReportWorkerInfoField.values()));
    mWorkerRange = WorkerRange.ALL;
  }

  /**
   * @return the client selected worker addresses
   */
  public Set<String> getAddresses() {
    return mAddresses;
  }

  /**
   * @return the field range of report worker info
   */
  public Set<ReportWorkerInfoField> getFieldRange() {
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
  public ReportWorkerOptions setAddresses(Set<String> addresses) {
    mAddresses = addresses;
    return this;
  }

  /**
   * @param fieldRange the field range of report worker info
   * @return the updated options object
   */
  public ReportWorkerOptions setFieldRange(Set<ReportWorkerInfoField> fieldRange) {
    mFieldRange = fieldRange;
    return this;
  }

  /**
   * @param workerRange the client selected worker range
   * @return the updated options object
   */
  public ReportWorkerOptions setWorkerRange(WorkerRange workerRange) {
    mWorkerRange = workerRange;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ReportWorkerOptions)) {
      return false;
    }
    ReportWorkerOptions that = (ReportWorkerOptions) o;
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
    return Objects.toStringHelper(this)
        .add("addresses", mAddresses)
        .add("fieldRange", mFieldRange)
        .add("workerRange", mWorkerRange)
        .toString();
  }

  /**
   * @return Thrift representation of the options
   */
  public GetReportWorkerInfoListTOptions toThrift() {
    GetReportWorkerInfoListTOptions options = new GetReportWorkerInfoListTOptions();
    options.setAddresses(mAddresses);
    if (mFieldRange != null) {
      Set<alluxio.thrift.ReportWorkerInfoField> thriftFieldRange = new HashSet<>();
      for (ReportWorkerInfoField field : mFieldRange) {
        thriftFieldRange.add(field.toThrift());
      }
      options.setFieldRange(thriftFieldRange);
    }
    options.setWorkerRange(mWorkerRange.toThrift());
    return options;
  }

  /**
   * Enum representing the range of workers that we want to show capacity information for.
   */
  public static enum WorkerRange {
    ALL,
    LIVE,
    LOST,
    SPECIFIED;

    /**
     * @return the thrift representation of this worker info filter type
     */
    public alluxio.thrift.WorkerRange toThrift() {
      return alluxio.thrift.WorkerRange.valueOf(name());
    }

    /**
     * @param workerRange the thrift representation of the worker range to create
     * @return the wire type version of the worker range
     */
    public static WorkerRange fromThrift(alluxio.thrift.WorkerRange workerRange) {
      return WorkerRange.valueOf(workerRange.name());
    }
  }

  /**
   * Enum representing the fields of the report worker information.
   */
  public static enum ReportWorkerInfoField {
    ADDRESS,
    CAPACITY_BYTES,
    CAPACITY_BYTES_ON_TIERS,
    ID,
    LAST_CONTACT_SEC,
    START_TIME_MS,
    STATE,
    USED_BYTES,
    USED_BYTES_ON_TIERS;

    /**
     * @return the thrift representation of this report worker info fields
     */
    public alluxio.thrift.ReportWorkerInfoField toThrift() {
      return alluxio.thrift.ReportWorkerInfoField.valueOf(name());
    }

    /**
     * @param variableRange the thrift representation of the report worker info fields
     * @return the wire type version of the report worker info field
     */
    public static ReportWorkerInfoField fromThrift(
        alluxio.thrift.ReportWorkerInfoField variableRange) {
      return ReportWorkerInfoField.valueOf(variableRange.name());
    }
  }
}
