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

package alluxio.master.backcompat.ops;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.master.backcompat.FsTestOp;
import alluxio.master.backcompat.Version;
import alluxio.security.authorization.AclEntry;
import alluxio.grpc.SetAclAction;

import java.util.Arrays;

/**
 * Test for setting an ACL.
 */
public final class SetAcl extends FsTestOp {
  private static final AlluxioURI DIR_SET = new AlluxioURI("/dirToSetAcl");
  private static final String ACL_STRING = "group:testgroup:rwx";

  private static final AlluxioURI DIR_ADD_REMOVE = new AlluxioURI("/dirToAddRemoveAcl");

  @Override
  public void apply(FileSystem fs) throws Exception {
    fs.createDirectory(DIR_SET);
    fs.setAcl(DIR_SET, SetAclAction.MODIFY, Arrays.asList(AclEntry.fromCliString(ACL_STRING)));

    fs.createDirectory(DIR_ADD_REMOVE);
    fs.setAcl(DIR_ADD_REMOVE, SetAclAction.MODIFY,
        Arrays.asList(AclEntry.fromCliString(ACL_STRING)));
    fs.setAcl(DIR_ADD_REMOVE, SetAclAction.REMOVE,
        Arrays.asList(AclEntry.fromCliString(ACL_STRING)));
  }

  @Override
  public void check(FileSystem fs) throws Exception {
    assertThat(fs.getStatus(DIR_SET).getAcl().toString(), containsString(ACL_STRING));
    assertThat(fs.getStatus(DIR_ADD_REMOVE).getAcl().toString(), not(containsString(ACL_STRING)));
  }

  @Override
  public boolean supportsVersion(Version version) {
    return version.compareTo(new Version(1, 9, 0)) >= 0;
  }
}
