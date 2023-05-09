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

package alluxio.master.file.mdsync;

import java.util.Optional;
import javax.annotation.Nullable;

/**
 * The overall result of a base task.
 */
public class BaseTaskResult {

  private final Throwable mT;

  BaseTaskResult(@Nullable Throwable t) {
    mT = t;
  }

  boolean succeeded() {
    return mT == null;
  }

  Optional<Throwable> getThrowable() {
    return Optional.ofNullable(mT);
  }
}
