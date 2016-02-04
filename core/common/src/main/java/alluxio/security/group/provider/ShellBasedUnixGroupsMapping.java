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

package alluxio.security.group.provider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import alluxio.conf.TachyonConf;
import alluxio.security.group.GroupMappingService;
import alluxio.util.CommonUtils;

/**
 * A simple shell-based implementation of {@link GroupMappingService} that exec's the {@code groups}
 * shell command to fetch the group memberships of a given user.
 */
public final class ShellBasedUnixGroupsMapping implements GroupMappingService {

  /**
   * Returns list of groups for a user.
   *
   * @param user get groups for this user
   * @return list of groups for a given user
   * @throws IOException when getting the UNIX groups
   */
  @Override
  public List<String> getGroups(String user) throws IOException {
    List<String> groups = CommonUtils.getUnixGroups(user);
    // remove duplicated primary group
    return new ArrayList<String>(new LinkedHashSet<String>(groups));
  }

  @Override
  public void setConf(TachyonConf conf) {
    // does nothing in this provider of user to groups mapping
  }
}
