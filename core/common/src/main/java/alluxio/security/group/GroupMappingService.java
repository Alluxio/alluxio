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

package alluxio.security.group;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.util.CommonUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Interface for Alluxio user-to-groups mapping. {@link GroupMappingService} allows for server to
 * get the various group memberships of a given user via the {@link #getGroups(String)} call, thus
 * ensuring a consistent user-to-groups mapping and protects against mapping inconsistencies between
 * servers and clients in an Alluxio cluster.
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
     * @param conf Alluxio configuration
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
