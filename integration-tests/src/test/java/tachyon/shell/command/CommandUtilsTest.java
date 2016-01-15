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

package tachyon.shell.command;

import org.junit.Assert;
import org.junit.Test;

public class CommandUtilsTest {

  @Test
  public void convertPermissionTest() {
    Assert.assertEquals("-rw-rw-rw-", CommandUtils.formatPermission(0666, false));
    Assert.assertEquals("drw-rw-rw-", CommandUtils.formatPermission(0666, true));
    Assert.assertEquals("-rwxrwxrwx", CommandUtils.formatPermission(0777, false));
    Assert.assertEquals("drwxrwxrwx", CommandUtils.formatPermission(0777, true));
    Assert.assertEquals("-r--r--r--", CommandUtils.formatPermission(0444, false));
    Assert.assertEquals("dr--r--r--", CommandUtils.formatPermission(0444, true));
    Assert.assertEquals("-r-xr-xr-x", CommandUtils.formatPermission(0555, false));
    Assert.assertEquals("dr-xr-xr-x", CommandUtils.formatPermission(0555, true));
    Assert.assertEquals("-rwxr-xr--", CommandUtils.formatPermission(0754, false));
    Assert.assertEquals("drwxr-xr--", CommandUtils.formatPermission(0754, true));
  }
}
