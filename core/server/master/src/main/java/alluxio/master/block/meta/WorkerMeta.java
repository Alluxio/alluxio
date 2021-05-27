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

package alluxio.master.block.meta;

import alluxio.util.CommonUtils;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import net.jcip.annotations.ThreadSafe;

import java.util.concurrent.atomic.AtomicLong;

/**
 * An object representation of the worker metadata.
 * This class is thread safe so accessing or updating the fields do not require locking.
 * The only mutable field is the last updated timestamp.
 */
@ThreadSafe
public class WorkerMeta {
  /** Worker's address. */
  final WorkerNetAddress mWorkerAddress;
  /** The id of the worker. */
  final long mId;
  /** Start time of the worker in ms. */
  final long mStartTimeMs;

  /**
   * Constructor.
   *
   * @param id the worker ID
   * @param address the worker address
   */
  public WorkerMeta(long id, WorkerNetAddress address) {
    mId = id;
    mWorkerAddress = Preconditions.checkNotNull(address, "address");
    mStartTimeMs = CommonUtils.getCurrentMs();
  }
}
