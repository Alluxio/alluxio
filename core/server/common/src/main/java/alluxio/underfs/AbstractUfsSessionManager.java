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

package alluxio.underfs;

import alluxio.AlluxioURI;
import alluxio.metrics.MetricsSystem;

import com.codahale.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Basic implementation of {@link UfsSessionManager}.
 */
@ThreadSafe
public abstract class AbstractUfsSessionManager implements UfsSessionManager {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractUfsSessionManager.class);

  private UfsManager mUfsManager;
  private ConcurrentHashMap<Long, Counter> mUfsUriToCounter;

  AbstractUfsSessionManager(UfsManager ufsManager) {
    mUfsManager = ufsManager;
    mUfsUriToCounter = new ConcurrentHashMap<>();
  }

  @Override
  public void openSession(long mountId) {
    mUfsUriToCounter.compute(mountId, (k, counter) -> {
      if (counter == null) {
        try {
          AlluxioURI key = mUfsManager.get(mountId).getUfsMountPointUri();
          counter = getWriteSessionsCounter(key);
        } catch (Exception e) {
          LOG.warn(e.getMessage());
        }
      }
      counter.inc();
      return counter;
    });
  }

  @Override
  public void closeSession(long mountId) {
    mUfsUriToCounter.computeIfPresent(mountId, (k, counter) -> {
      counter.dec();
      if (counter.getCount() == 0) {
        // Remove key
        return null;
      }
      return counter;
    });
  }

  /**
   * Get the counter for tracking active writes to the ufs.
   *
   * @param ufsUri the ufs being written to
   * @return the active write counter
   */
  private static Counter getWriteSessionsCounter(AlluxioURI ufsUri) {
    String ufsString = MetricsSystem.escape(ufsUri);
    String activeWriteMetricName = String.format("ActiveUfsWriteCount-Ufs:%s", ufsString);
    return MetricsSystem.workerCounter(activeWriteMetricName);
  }
}
