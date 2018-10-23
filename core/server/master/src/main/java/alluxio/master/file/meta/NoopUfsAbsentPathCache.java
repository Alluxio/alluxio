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

package alluxio.master.file.meta;

import alluxio.AlluxioURI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This is a noop version of {@link UfsAbsentPathCache}.
 */
@ThreadSafe
public final class NoopUfsAbsentPathCache implements UfsAbsentPathCache {
  private static final Logger LOG = LoggerFactory.getLogger(NoopUfsAbsentPathCache.class);

  /**
   * Creates a new instance of {@link NoopUfsAbsentPathCache}.
   */
  public NoopUfsAbsentPathCache() {
    // Do nothing
  }

  @Override
  public void process(AlluxioURI path, List<InodeView> prefixInodes) {
    // Do nothing
  }

  @Override
  public void processExisting(AlluxioURI path) {
    // Do nothing
  }

  @Override
  public boolean isAbsent(AlluxioURI path) {
    return false;
  }
}
