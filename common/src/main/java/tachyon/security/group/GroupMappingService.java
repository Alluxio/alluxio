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

import tachyon.conf.TachyonConf;

/**
 * An interface for the implementation of a user-to-groups mapping service used by
 * {@link UserToGroupsMappingService}.
 */
public interface GroupMappingService {
  /**
   * Gets all various group memberships of a given user. Returns EMPTY list in case of non-existing
   * user.
   *
   * @param user user's name
   * @return group memberships of user
   * @throws IOException if can't get user's groups
   */
  public List<String> getGroups(String user) throws IOException;

  /**
   * Sets the configuration to GroupMappingService. For example, when we get user-groups mapping
   * from LDAP, we will need configuration to set up the connection to LDAP server.
   *
   * @param conf The tachyon configuration set to GroupMappingService
   * @throws IOException if failed config GroupMappingService
   */
  public void setConf(TachyonConf conf) throws IOException;
}
