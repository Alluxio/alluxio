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

package alluxio.security.group.provider;

import alluxio.security.group.GroupMappingService;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * A simple implementation of {@link GroupMappingService} that returns a group which is same name
 * with the given user name.
 */
public final class IdentityUserGroupsMapping implements GroupMappingService {

  /**
   * Constructs a new {@link IdentityUserGroupsMapping}.
   */
  public IdentityUserGroupsMapping() {}

  /**
   * Returns list of groups for a user.
   *
   * @param user get groups for this user
   * @return list of groups for a given user
   */
  @Override
  public List<String> getGroups(String user) {
    return Lists.newArrayList(user);
  }
}
