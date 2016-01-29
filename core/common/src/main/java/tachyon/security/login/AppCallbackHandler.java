/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.security.login;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

import tachyon.Constants;
import tachyon.conf.TachyonConf;

/**
 * A callback handler for {@link AppLoginModule}.
 */
@NotThreadSafe
public final class AppCallbackHandler implements CallbackHandler {
  private String mUserName;

  /**
   * @param conf the configuration for Tachyon
   */
  public AppCallbackHandler(TachyonConf conf) {
    if (conf.containsKey(Constants.SECURITY_LOGIN_USERNAME)) {
      mUserName = conf.get(Constants.SECURITY_LOGIN_USERNAME);
    } else {
      mUserName = "";
    }
  }

  @Override
  public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
    for (Callback callback : callbacks) {
      if (callback instanceof NameCallback) {
        NameCallback nameCallback = (NameCallback) callback;
        nameCallback.setName(mUserName);
      } else {
        Class<?> callbackClass = (callback == null) ? null : callback.getClass();
        throw new UnsupportedCallbackException(callback, callbackClass + " is unsupported.");
      }
    }
  }
}
