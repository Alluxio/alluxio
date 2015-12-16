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

package tachyon.security.group.provider;

import java.io.IOException;
import java.util.List;

import tachyon.conf.TachyonConf;
import tachyon.security.group.GroupMappingServiceProvider;
import tachyon.util.CommonUtils;

/**
 * A simple shell-based implementation of {@link GroupMappingServiceProvider} that exec's the
 * <code>groups</code> shell command to fetch the group memberships of a given user.
 */
public final class ShellBasedUnixGroupsMapping implements GroupMappingServiceProvider {

  /**
   * Returns list of groups for a user.
   *
   * @param user get groups for this user
   * @return list of groups for a given user
   */
  @Override
  public List<String> getGroups(String user) throws IOException {
    List<String> groups = CommonUtils.getUnixGroups(user);
    // remove duplicated primary group
    if (groups != null && groups.size() > 1) {
      for (int i = 1; i < groups.size(); i++) {
        if (groups.get(i).equals(groups.get(0))) {
          groups.remove(i);
          break;
        }
      }
    }
    return groups;
  }

  @Override
  public void setConf(TachyonConf conf) {
    // does nothing in this provider of user to groups mapping
  }

}
