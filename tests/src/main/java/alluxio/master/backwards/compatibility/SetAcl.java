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

package alluxio.master.backwards.compatibility;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.security.authorization.AclEntry;
import alluxio.wire.SetAclAction;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;

import java.util.Arrays;

class SetAcl extends FsTestOp {
  private static final AlluxioURI DIR = new AlluxioURI("/dirToSetAcl");
  private static final String ACL_STRING = "group::rwx";

  @Override
  public void apply(FileSystem fs) throws Exception {
    fs.createDirectory(DIR);
    fs.setAcl(DIR, SetAclAction.MODIFY, Arrays.asList(AclEntry.fromCliString(ACL_STRING)));
  }

  @Override
  public void check(FileSystem fs) throws Exception {
    Assert.assertThat("Acl should be set", fs.getStatus(DIR).getAcl().toString(),
        CoreMatchers.containsString(ACL_STRING));
  }

  @Override
  public boolean supportsVersion(Version version) {
    return version.compareTo(new Version(1, 9, 0)) >= 0;
  }
}
