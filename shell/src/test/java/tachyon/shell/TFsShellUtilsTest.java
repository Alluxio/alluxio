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

package tachyon.shell;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import tachyon.Constants;
import tachyon.conf.TachyonConf;

/**
 * Unit tests on tachyon.command.Utils.
 *
 * Note that the test case for validatePath() is already covered in getFilePath. Hence only
 * getFilePathTest is specified.
 */
public class TFsShellUtilsTest {

  @Test
  public void getFilePathTest() throws IOException {
    String[] paths =
        new String[] {Constants.HEADER + "localhost:19998/dir",
            Constants.HEADER_FT + "localhost:19998/dir", "/dir", "dir"};
    String expected = "/dir";
    for (String path : paths) {
      String result = TFsShellUtils.getFilePath(path, new TachyonConf());
      Assert.assertEquals(expected, result);
    }
  }
}
