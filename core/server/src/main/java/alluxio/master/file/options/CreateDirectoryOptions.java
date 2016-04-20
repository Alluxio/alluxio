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

import alluxio.master.MasterContext;
import alluxio.security.authorization.PermissionStatus;
import alluxio.thrift.CreateDirectoryTOptions;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a directory.
 */
@NotThreadSafe
public final class CreateDirectoryOptions extends CreatePathOptions<CreateDirectoryOptions> {
  private boolean mAllowExists;

  /**
   * @return the default {@link CreateDirectoryOptions}
   * @throws IOException if I/O error occurs
   */
  public static CreateDirectoryOptions defaults() throws IOException {
    return new CreateDirectoryOptions();
  }

  /**
   * Creates a new instance of {@link CreateDirectoryOptions} from {@link CreateDirectoryTOptions}.
   * The option of permission status is constructed with the username obtained from thrift
   * transport.
   *
   * @param tOptions the {@link CreateDirectoryTOptions} to use
   * @return the {@link CreateDirectoryOptions} from {@link CreateDirectoryTOptions}
   * @throws IOException if an I/O error occurs
   */
  public static CreateDirectoryOptions fromTOptions(CreateDirectoryTOptions tOptions)
      throws IOException {
    CreateDirectoryOptions options = new CreateDirectoryOptions();
    options.setAllowExists(tOptions.isAllowExists());
    options.setPersisted(tOptions.isPersisted());
    options.setRecursive(tOptions.isRecursive());
    options.setPermissionStatus(
        PermissionStatus.defaults().setUserFromThriftClient(MasterContext.getConf()));
    return options;
  }

  private CreateDirectoryOptions() throws IOException {
    super();
    mAllowExists = false;
  }

  /**
   * @return the allowExists flag; it specifies whether an exception should be thrown if the object
   *         being made already exists
   */
  public boolean isAllowExists() {
    return mAllowExists;
  }

  /**
   * @param allowExists the allowExists flag value to use; it specifies whether an exception
   *        should be thrown if the object being made already exists.
   * @return the updated options object
   */
  public CreateDirectoryOptions setAllowExists(boolean allowExists) {
    mAllowExists = allowExists;
    return this;
  }

  @Override
  protected CreateDirectoryOptions getThis() {
    return this;
  }

  /**
   * @return the name : value pairs for all the fields
   */
  @Override
  public String toString() {
    return toStringHelper().add("allowExists", mAllowExists).toString();
  }
}
