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

package alluxio.master.file.options;

import alluxio.util.ModeUtils;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Collections;
import java.util.List;

/**
 * Method options for creating a directory.
 */
@NotThreadSafe
public final class CreateDirectoryOptions
    extends alluxio.file.options.CreateDirectoryOptions<CreateDirectoryOptions> {
  /**
   * @return the default {@link CreateDirectoryOptions}
   */
  public static CreateDirectoryOptions defaults() {
    return new CreateDirectoryOptions();
  }

  private CreateDirectoryOptions() {
    super();

    CreatePathOptionsUtils.setDefaults(this);

    mAllowExists = false;
    mMode = ModeUtils.applyDirectoryUMask(mMode);
  }

  @Override
  protected CreateDirectoryOptions getThis() {
    return this;
  }
}
