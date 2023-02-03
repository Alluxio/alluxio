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

package alluxio.master.file;

import alluxio.exception.AccessControlException;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.security.authorization.Mode;
import org.apache.commons.lang3.NotImplementedException;

/**
 * A dummy implementation of the interface of {@link PermissionChecker} for demo use only.
 */
public final class SamplePermissionChecker implements PermissionChecker {

  // This variable is not used. It is just to show that the InodeTree variable
  // is available to use
  private InodeTree inodeTree;

  public SamplePermissionChecker(InodeTree inodeTree) {
    this.inodeTree = inodeTree;
  }

  public void checkParentPermission(Mode.Bits bits, LockedInodePath inodePath)
          throws AccessControlException, InvalidPathException {
    throw new NotImplementedException();
  }

  public void checkPermission(Mode.Bits bits, LockedInodePath inodePath)
          throws AccessControlException {
    // The user ID should be get from somewhere else. Here it is hardcoded.
    if (!new PolicyChecker().check(inodePath, "foo@bar.com")) {
      throw new AccessControlException(inodePath + "cannot be accessed");
    }
  }

  public void checkSuperUser() throws AccessControlException {
    throw new NotImplementedException();
  }

  public Mode.Bits getPermission(LockedInodePath inodePath) {
    throw new NotImplementedException();
  }

  public void checkSetAttributePermission(LockedInodePath inodePath, boolean superuserRequired,
                                   boolean ownerRequired, boolean writeRequired)
          throws AccessControlException, InvalidPathException {
    throw new NotImplementedException();
  }

  static class PolicyChecker {

    //we can get policies from remote policy store in real use cases. The policy can be table based.
    public void getPolicies() {
      // we leave it as blank just for demo
    }

    public boolean check(LockedInodePath inodePath, String uid) {
      String path = inodePath.getUri().getPath();
      /** For demo only, we only allow the path is /a/b/c for user foo@bar.com.
       * other paths and other users will be rejected.
       *
       * In reality, the policy can determine based on if the path is under a given
       * table and if a user has that table.
       */
      return (path != null &&
              path.equalsIgnoreCase("/a/b/c") &&
              uid != null &&
              uid.equalsIgnoreCase("foo@bar.com"));
    }
  }
}
