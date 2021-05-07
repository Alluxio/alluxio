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

package alluxio.security.user;

import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.security.User;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import javax.security.auth.Subject;

/**
 * Base implementation of {@link UserState}.
 */
public abstract class BaseUserState implements UserState {
  private static final Logger LOG = LoggerFactory.getLogger(BaseUserState.class);

  protected final Subject mSubject;
  protected volatile User mUser;
  // TODO(gpang): consider removing conf, and simply passing in implementation-specific values.
  protected AlluxioConfiguration mConf;

  /**
   * @param subject the subject
   * @param conf the Alluxio configuration
   */
  public BaseUserState(Subject subject, AlluxioConfiguration conf) {
    mSubject = subject;
    mConf = conf;
    mUser = getUserFromSubject();
  }

  @Override
  public Subject getSubject() {
    try {
      tryLogin();
    } catch (UnauthenticatedException e) {
      if (!LOG.isDebugEnabled()) {
        LOG.warn("Subject login failed from {}: {}", this.getClass().getName(), e.toString());
      } else {
        LOG.error(String.format("Subject login failed from %s: ", this.getClass().getName()), e);
      }
    }
    return mSubject;
  }

  @Override
  public User getUser() throws UnauthenticatedException {
    tryLogin();
    return mUser;
  }

  protected abstract User login() throws UnauthenticatedException;

  private void tryLogin() throws UnauthenticatedException {
    if (mUser == null) {
      synchronized (this) {
        if (mUser == null) {
          mUser = login();
        }
      }
    }
  }

  /**
   * @return the {@link User} found in the subject, or null if the subject does not contain one
   */
  private User getUserFromSubject() {
    Set<User> userSet = mSubject.getPrincipals(User.class);
    if (userSet.isEmpty()) {
      return null;
    }
    return userSet.iterator().next();
  }

  @Override
  public User relogin() throws UnauthenticatedException {
      // Specific implementations can change the behavior.
    tryLogin();
    return mUser;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mSubject);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null) {
      return false;
    }

    if (this.getClass() != o.getClass()) {
      return false;
    }
    BaseUserState that = (BaseUserState) o;
    return Objects.equal(mSubject, that.mSubject);
  }
}
