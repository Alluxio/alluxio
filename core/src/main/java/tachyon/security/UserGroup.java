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

package tachyon.security;

import java.io.IOException;
import java.security.Principal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import tachyon.Constants;
import tachyon.conf.TachyonConf;

public class UserGroup {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final String OS_LOGIN_MODULE_NAME;
  private static final Class<? extends Principal> OS_PRINCIPAL_CLASS;
  private static final boolean WINDOWS =
      System.getProperty("os.name").startsWith("Windows");
  private static final boolean IS_64_BIT =
      System.getProperty("os.arch").contains("64");
  private static final boolean AIX = System.getProperty("os.name").equals("AIX");
  public static final String JAVA_VENDOR_NAME = System.getProperty("java.vendor");
  public static final boolean IBM_JAVA = JAVA_VENDOR_NAME.contains("IBM");

  static {
    OS_LOGIN_MODULE_NAME = getOSLoginModuleName();
    OS_PRINCIPAL_CLASS = getOsPrincipalClass();
  }

  /**
   * Information about the logged in user.
   */
  private static UserGroup sLoginUser = null;
  private static Groups sGroups;
  /** The configuration to use */
  private static TachyonConf sConf;
  //TODO: represent the user and group by a class
  private final User mUser;
  private final Subject mSubject;

  /* Return the OS login module class name */
  private static String getOSLoginModuleName() {
    if (IBM_JAVA) {
      if (WINDOWS) {
        return IS_64_BIT ? "com.ibm.security.auth.module.Win64LoginModule"
            : "com.ibm.security.auth.module.NTLoginModule";
      } else if (AIX) {
        return IS_64_BIT ? "com.ibm.security.auth.module.AIX64LoginModule"
            : "com.ibm.security.auth.module.AIXLoginModule";
      } else {
        return "com.ibm.security.auth.module.LinuxLoginModule";
      }
    } else {
      return WINDOWS ? "com.sun.security.auth.module.NTLoginModule"
          : "com.sun.security.auth.module.UnixLoginModule";
    }
  }

  private static Class<? extends Principal> getOsPrincipalClass() {
    ClassLoader cl = ClassLoader.getSystemClassLoader();
    try {
      String principalClass = null;
      if (IBM_JAVA) {
        if (IS_64_BIT) {
          principalClass = "com.ibm.security.auth.UsernamePrincipal";
        } else {
          if (WINDOWS) {
            principalClass = "com.ibm.security.auth.NTUserPrincipal";
          } else if (AIX) {
            principalClass = "com.ibm.security.auth.AIXPrincipal";
          } else {
            principalClass = "com.ibm.security.auth.LinuxPrincipal";
          }
        }
      } else {
        principalClass = WINDOWS ? "com.sun.security.auth.NTUserPrincipal"
            : "com.sun.security.auth.UnixPrincipal";
      }
      return (Class<? extends Principal>) cl.loadClass(principalClass);
    } catch (ClassNotFoundException e) {
      LOG.error("Unable to find JAAS classes:" + e.getMessage());
    }
    return null;
  }

  private static class TachyonJaasConfiguration extends Configuration{
    private static final Map<String, String> BASIC_JAAS_OPTIONS =
        new HashMap<String,String>();

    private static final AppConfigurationEntry OS_SPECIFIC_LOGIN =
        new AppConfigurationEntry(OS_LOGIN_MODULE_NAME,
            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
            BASIC_JAAS_OPTIONS);
    private static final AppConfigurationEntry TACHYON_LOGIN =
        new AppConfigurationEntry(TachyonLoginModule.class.getName(),
            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
            BASIC_JAAS_OPTIONS);

    private static final AppConfigurationEntry[] SIMPLE = new
        AppConfigurationEntry[]{OS_SPECIFIC_LOGIN, TACHYON_LOGIN};

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
      if ("simple".equalsIgnoreCase(appName)) {
        return SIMPLE;
      } else if ("kerbores".equalsIgnoreCase(appName)) {
        // TODO: kerbores
      }
      return null;
    }
  }

  public static class TachyonLoginModule implements LoginModule {
    Subject mSubject;

    @Override
    public boolean abort() throws LoginException {
      return true;
    }

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler,
                           Map<String, ?> sharedState, Map<String, ?> options) {
      this.mSubject = subject;
    }

    @Override
    public boolean login() throws LoginException {
      return true;
    }

    @Override
    public boolean logout() throws LoginException {
      return false;
    }

    private <T extends Principal> T getCanonicalUser(Class<T> cls) {
      for (T user: mSubject.getPrincipals(cls)) {
        return user;
      }
      return null;
    }

    @Override
    public boolean commit() throws LoginException {
      if (!mSubject.getPrincipals(User.class).isEmpty()) {
        return true;
      }

      Principal user = null;

      //TODO: 1. kerbores, 2. env

      //use OS user
      if (user == null) {
        user = getCanonicalUser(OS_PRINCIPAL_CLASS);
      }

      if (user != null) {
        User userEntry = new User(user.getName());
        mSubject.getPrincipals().add(userEntry);
        return true;
      }

      throw new LoginException("Cannot find user");
    }
  }

  public static void ensureInitialized() {
    if (sConf == null) {
      synchronized (UserGroup.class) {
        if (sConf == null) {
          initialized(new TachyonConf());
        }
      }
    }
  }

  public static void setConfiguration(TachyonConf conf) {
    initialized(conf);
  }

  public static void initialized(TachyonConf conf) {
    sGroups = Groups.getUserToGroupsMappingService(conf);
    UserGroup.sConf = conf;
  }

  public static synchronized void loginUserFromSubject() throws IOException {
    ensureInitialized();
    try {
      Subject subject = new Subject();

      // TODO: support Kerbores.
      // TODO: create LoginContext based on security conf. Hard code "simple" here temporarily.
      LoginContext loginContext = newLoginContext("simple", subject,
          new TachyonJaasConfiguration());
      loginContext.login();

      sLoginUser = new UserGroup(subject);
    } catch (LoginException e) {
      throw new IOException("fail to login", e);
    }
  }

  /**
   * Create a user from a login name. It is intended to be used for remote
   * users in RPC
   * @param user the full user principal name, must not be empty or null
   * @return the UserGroup for the remote user.
   */
  public static UserGroup createRemoteUser(String user) {
    if (user == null || user.isEmpty()) {
      throw new IllegalArgumentException("Null user");
    }
    Subject subject = new Subject();
    subject.getPrincipals().add(new User(user));
    UserGroup result = new UserGroup(subject);
    return result;
  }

  /**
   * Create a UGI for testing
   * @param user the full user principal name
   * @param groups the names of the groups that the user belongs to
   * @return a fake user for running unit tests
   */
  @VisibleForTesting
  public static UserGroup createTestUser(String user, String...groups) {
    ensureInitialized();
    TestingGroups testGroups = new TestingGroups(sGroups);
    testGroups.setUserGroups(user, groups);
    sGroups = testGroups;

    Subject subject = new Subject();
    subject.getPrincipals().add(new User(user));
    UserGroup result = new UserGroup(subject);
    return result;
  }

  public static synchronized UserGroup getTachyonLoginUser()
      throws IOException {
    if (sLoginUser == null) {
      loginUserFromSubject();
    }
    return sLoginUser;
  }

  private static LoginContext newLoginContext(String appName, Subject subject,
                                       Configuration conf) throws LoginException {
    Thread t = Thread.currentThread();
    ClassLoader oldCCL = t.getContextClassLoader();
    t.setContextClassLoader(TachyonLoginModule.class.getClassLoader());
    try {
      return new LoginContext(appName, subject, null, conf);
    } finally {
      t.setContextClassLoader(oldCCL);
    }
  }

  private UserGroup(Subject subject) {
    this.mSubject = subject;
    this.mUser = subject.getPrincipals(User.class).iterator().next();
  }

  /**
   * Get the user's login name.
   * @return the user's name up to the first '/' or '@'.
   */
  public String getShortUserName() {
    for (User p: mSubject.getPrincipals(User.class)) {
      return p.getName();
    }
    return null;
  }

  /**
   * Get the group names for this user.
   * @return the list of users with the primary group first. If the command
   *    fails, it returns an empty list.
   */
  public synchronized List<String> getGroupNames() {
    ensureInitialized();
    try {
      Set<String> result = new LinkedHashSet<String>(sGroups.getGroups(getShortUserName()));
      return Lists.newArrayList(result);
    } catch (IOException ie) {
      LOG.warn("No groups available for user " + getShortUserName());
      return Lists.newArrayList();
    }
  }

  public String getPrimaryGroupName() throws IOException {
    List<String> groups = getGroupNames();
    if (groups.isEmpty()) {
      throw new IOException("There is no primary group for UGI " + this);
    }
    return groups.get(0);
  }

  /**
   * This class is used for storing the groups for testing. It stores a local
   * map that has the translation of usernames to groups.
   */
  private static class TestingGroups extends Groups {
    private final Map<String, List<String>> mUserToGroupsMapping =
        new HashMap<String,List<String>>();
    private Groups mImpl;

    private TestingGroups(Groups impl) {
      mImpl = impl;
    }

    @Override
    public List<String> getGroups(String user) throws IOException {
      List<String> result = mUserToGroupsMapping.get(user);
      if (result == null) {
        return mImpl.getGroups(user);
      }
      return result;
    }

    private void setUserGroups(String user, String...groups) {
      mUserToGroupsMapping.put(user, Arrays.asList(groups));
    }
  }
}
