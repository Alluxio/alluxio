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
import alluxio.util.CommonUtils;
import alluxio.util.interfaces.IOFunction;

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
/**
 * A simple shell-based implementation of {@link GroupMappingService} that exec's the {@code groups}
 * shell command to fetch the group memberships of a given user.
 */
public final class ShellBasedUnixGroupsMapping implements GroupMappingService {

  private final IOFunction<String, List<String>> mGroupMappingProvider;

  /**
   * Constructs a new {@link ShellBasedUnixGroupsMapping} with the information provider.
   *
   * @param mappingProvider a function which supplies a list of group names given a username
   */
  @VisibleForTesting
  public ShellBasedUnixGroupsMapping(IOFunction<String, List<String>> mappingProvider) {
    mGroupMappingProvider = mappingProvider;
  }

  /**
   * Constructs a new {@link ShellBasedUnixGroupsMapping}.
   */
  public ShellBasedUnixGroupsMapping() {
    mGroupMappingProvider = CommonUtils::getUnixGroups;
  }

  /**
   * Returns list of groups for a user.
   *
   * @param user get groups for this user
   * @return list of groups for a given user
   */
  @Override
  public List<String> getGroups(String user) throws IOException {
    List<String> groups = mGroupMappingProvider.apply(user);
    // remove duplicated primary group
    return new ArrayList<>(new LinkedHashSet<>(groups));
  }
}
