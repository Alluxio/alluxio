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

package alluxio.master.meta;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Metadata for an Alluxio standby masters.
 * This class is not thread safe, so external locking is required.
 */
@NotThreadSafe
public final class MasterInfo {
  private static final Logger LOG = LoggerFactory.getLogger(MasterInfo.class);

  /**
   * Master's hostname.
   */
  private final String mHostname;
  /**
   * The id of the master.
   */
  private final long mId;
  /**
   * Master's last updated time in ms.
   */
  private long mLastUpdatedTimeMs;

  /**
   * Creates a new instance of {@link MasterInfo}.
   *
   * @param id       the master id to use
   * @param hostname the master hostname to use
   */
  public MasterInfo(long id, String hostname) {
    mHostname = Preconditions.checkNotNull(hostname, "hostname");
    mId = id;
    mLastUpdatedTimeMs = System.currentTimeMillis();
  }

  /**
   * @return the master's hostname
   */
  public String getHostname() {
    return mHostname;
  }

  /**
   * @return the id of the master
   */
  public long getId() {
    return mId;
  }

  /**
   * @return the last updated time of the master in ms
   */
  public long getLastUpdatedTimeMs() {
    return mLastUpdatedTimeMs;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("id", mId).add("hostname", mHostname)
        .add("lastUpdatedTimeMs", mLastUpdatedTimeMs).toString();
  }

  /**
   * Updates the last updated time of the master in ms.
   */
  public void updateLastUpdatedTimeMs() {
    mLastUpdatedTimeMs = System.currentTimeMillis();
  }
}
