/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file.options;

import alluxio.thrift.CreateDirectoryTOptions;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a directory.
 */
@NotThreadSafe
public final class CreateDirectoryOptions extends CreatePathOptions<CreateDirectoryOptions> {

  /**
   * @return the default {@link CreateDirectoryOptions}
   * @throws IOException if I/O error occurs
   */
  public static CreateDirectoryOptions defaults() throws IOException {
    return new CreateDirectoryOptions();
  }

  /**
   * Creates a new instance of {@link CreateDirectoryOptions} from {@link CreateDirectoryTOptions}.
   *
   * @param options the {@link CreateDirectoryTOptions} to use
   * @throws IOException if an I/O error occurs
   */
  public CreateDirectoryOptions(CreateDirectoryTOptions options) throws IOException {
    super();
    mAllowExists = options.isAllowExists();
    mDirectory = true;
    mPersisted = options.isPersisted();
    mRecursive = options.isRecursive();
  }

  @Override
  protected CreateDirectoryOptions getThis() {
    return this;
  }

  private CreateDirectoryOptions() throws IOException {
    super();
    mDirectory = true;
  }
}
