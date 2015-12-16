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

package tachyon.security.group;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.util.CommonUtils;

/**
 * A user-to-groups mapping service.
 *
 * {@link Groups} allows for server to get the various group memberships of a given user via the
 * {@link #getGroups(String)} call, thus ensuring a consistent user-to-groups mapping and protects
 * against vagaries of different mappings on servers and clients in a Tachyon cluster.
 */
public class Groups {
  private static final Logger LOG = LoggerFactory.getLogger(Groups.class);

  private final GroupMappingServiceProvider mImpl;

  public Groups() {
    this(new TachyonConf());
  }

  public Groups(TachyonConf conf) {
    try {
      mImpl =
          CommonUtils.createNewClassInstance(
              conf.<GroupMappingServiceProvider>getClass(Constants.SECURITY_GROUP_MAPPING), null,
              null);
      mImpl.setConf(conf);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the group memberships of a given user.
   *
   * @param user User's name
   * @return the group memberships of the user
   * @throws IOException if user does not exist
   */
  public List<String> getGroups(final String user) throws IOException {
    return mImpl.getGroups(user);
  }

  private static Groups sGROUPS = null;

  /**
   * Get the groups being used to map user-to-groups.
   *
   * @return the groups being used to map user-to-groups
   */
  public static Groups getUserToGroupsMappingService() {
    return getUserToGroupsMappingService(new TachyonConf());
  }

  /**
   * Get the groups being used to map user-to-groups.
   *
   * @param conf
   * @return the groups being used to map user-to-groups
   */
  public static synchronized Groups getUserToGroupsMappingService(TachyonConf conf) {
    if (sGROUPS == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(" Creating new Groups object");
      }
      sGROUPS = new Groups(conf);
    }
    return sGROUPS;
  }

}
