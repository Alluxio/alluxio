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

import alluxio.thrift.CreateFileTOptions;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a file.
 */
@NotThreadSafe
public final class CreateFileOptions extends CreatePathOptions<CreateFileOptions> {

  /**
   * @return the default {@link CreateFileOptions}
   * @throws IOException if I/O error occurs
   */
  public static CreateFileOptions defaults() throws IOException {
    return new CreateFileOptions();
  }

  /**
   * Creates a new instance of {@link CreateFileOptions} from {@link CreateFileTOptions}.
   *
   * @param options the {@link CreateFileTOptions} to use
   * @throws IOException if an I/O error occurs
   */
  public CreateFileOptions(CreateFileTOptions options) throws IOException {
    super();
    mBlockSizeBytes = options.getBlockSizeBytes();
    mDirectory = false;
    mPersisted = options.isPersisted();
    mRecursive = options.isRecursive();
    mTtl = options.getTtl();
  }

  @Override
  protected CreateFileOptions getThis() {
    return this;
  }

  private CreateFileOptions() throws IOException {
    super();
    mDirectory = false;
  }
}
