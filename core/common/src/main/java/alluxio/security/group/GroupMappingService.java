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

package alluxio.security.group;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.Configuration;
import alluxio.util.CommonUtils;

/**
 * Interface for Tachyon user-to-groups mapping. {@link GroupMappingService} allows for server to
 * get the various group memberships of a given user via the {@link #getGroups(String)} call, thus
 * ensuring a consistent user-to-groups mapping and protects against mapping inconsistencies between
 * servers and clients in a Tachyon cluster.
 */
@PublicApi
public interface GroupMappingService {

  /**
   * Factory for creating a new instance.
   */
  class Factory {
    private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

    /**
     * Gets the groups being used to map user-to-groups.
     *
     * @return the groups being used to map user-to-groups
     */
    public static GroupMappingService getUserToGroupsMappingService() {
      return getUserToGroupsMappingService(new Configuration());
    }

    /**
     * Gets the groups being used to map user-to-groups.
     *
     * @param conf Tachyon configuration
     * @return the groups being used to map user-to-groups
     */
    public static GroupMappingService getUserToGroupsMappingService(Configuration conf) {
      GroupMappingService mGroupMappingService;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Creating new Groups object");
      }
      try {
        mGroupMappingService =
            CommonUtils.createNewClassInstance(
                conf.<GroupMappingService>getClass(Constants.SECURITY_GROUP_MAPPING), null, null);
        mGroupMappingService.setConf(conf);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return mGroupMappingService;
    }
  }

  /**
   * Gets all various group memberships of a given user. Returns EMPTY list in case of non-existing
   * user.
   *
   * @param user user's name
   * @return group memberships of user
   * @throws IOException if can't get user's groups
   */
  List<String> getGroups(String user) throws IOException;

  /**
   * Sets the configuration to GroupMappingService. For example, when we get user-groups mapping
   * from LDAP, we will need configuration to set up the connection to LDAP server.
   *
   * @param conf The alluxio configuration set to GroupMappingService
   * @throws IOException if failed config GroupMappingService
   */
  void setConf(Configuration conf) throws IOException;
}
