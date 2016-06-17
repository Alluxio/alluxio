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

package alluxio.client.file.options;

import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.client.ClientContext;
import alluxio.security.authorization.PermissionStatus;
import alluxio.thrift.CompleteUfsFileTOptions;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Options for completing a UFS file. Currently we do not allow users to set arbitrary user and
 * group options. The user and group will be set to the user login.
 */
@PublicApi
@NotThreadSafe
public final class CompleteUfsFileOptions {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  /** The ufs user this file should be owned by. */
  private final String mUser;
  /** The ufs group this file should be owned by. */
  private final String mGroup;
  /** The ufs permission in Posix string format, such as 0777. */
  private String mPosixPerm;

  /**
   * @return the default {@link CompleteUfsFileOptions}
   */
  public static CompleteUfsFileOptions defaults() {
    PermissionStatus ps = PermissionStatus.defaults();
    try {
      // Set user and group from user login module.
      ps.setUserFromLoginModule(ClientContext.getConf()).applyFileUMask(ClientContext.getConf());
      // TODO(chaomin): set permission depends on the alluxio file.
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    // DEBUG(chaomin)
    LOG.info("CompleteUfsFileOptions ps = {} ", ps.toString());
    // END DEBUG
    return new CompleteUfsFileOptions(ps.getUserName(), ps.getGroupName(),
        String.valueOf(ps.getPermission().toShort()));
  }

  private CompleteUfsFileOptions(String user, String group, String posixPerm) {
    mUser = user;
    mGroup = group;
    mPosixPerm = posixPerm;
  }

  /**
   * @return the group which should own the file
   */
  public String getGroup() {
    return mGroup;
  }

  /**
   * @return the user who should own the file
   */
  public String getUser() {
    return mUser;
  }

  /**
   * @return the ufs permission in Posix string format
   */
  public String getPosixPerm() {
    return mPosixPerm;
  }

  /**
   * @return if the group has been set
   */
  public boolean hasGroup() {
    return mGroup != null;
  }

  /**
   * @return if the user has been set
   */
  public boolean hasUser() {
    return mUser != null;
  }

  /**
   * @return if the posixPerm has been set
   */
  public boolean hasPosixPerm() {
    return mPosixPerm != null;
  }

  /**
   * Sets the posix permission.
   *
   * @param posixPerm the permission to set
   */
  public void setPosixPerm(String posixPerm) {
    mPosixPerm = posixPerm;
  }

  @Override
  public boolean equals(Object o) {
    return this == o || o instanceof CompleteUfsFileOptions;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).toString();
  }

  /**
   * @return Thrift representation of the options
   */
  public CompleteUfsFileTOptions toThrift() {
    CompleteUfsFileTOptions options = new CompleteUfsFileTOptions();
    if (hasGroup()) {
      options.setGroup(mGroup);
    }
    if (hasUser()) {
      options.setUser(mUser);
    }
    if (hasPosixPerm()) {
      options.setPosixPerm(mPosixPerm);
    }
    return options;
  }
}
