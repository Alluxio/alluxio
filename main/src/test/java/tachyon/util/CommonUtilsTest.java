/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import tachyon.Constants;

public class CommonUtilsTest {
  @Test
  public void addLeadingZeroTest() throws IOException {
    for (int k = 0; k < 10; k ++) {
      Assert.assertEquals("" + k, CommonUtils.addLeadingZero(k, 1));
      Assert.assertEquals("0" + k, CommonUtils.addLeadingZero(k, 2));
      Assert.assertEquals("00" + k, CommonUtils.addLeadingZero(k, 3));
      Assert.assertEquals("000" + k, CommonUtils.addLeadingZero(k, 4));
      Assert.assertEquals("0000" + k, CommonUtils.addLeadingZero(k, 5));
    }
    for (int k = 10; k < 100; k ++) {
      Assert.assertEquals("" + k, CommonUtils.addLeadingZero(k, 1));
      Assert.assertEquals("" + k, CommonUtils.addLeadingZero(k, 2));
      Assert.assertEquals("0" + k, CommonUtils.addLeadingZero(k, 3));
      Assert.assertEquals("00" + k, CommonUtils.addLeadingZero(k, 4));
      Assert.assertEquals("000" + k, CommonUtils.addLeadingZero(k, 5));
    }
  }

  @Test(expected = IOException.class)
  public void addLeadingZeroTestWithNegativeNumber() throws IOException {
    CommonUtils.addLeadingZero(-1, 1);
  }

  @Test(expected = IOException.class)
  public void addLeadingZeroTestWithNegativeWidth() throws IOException {
    CommonUtils.addLeadingZero(1, 0);
  }

  @Test(expected = IOException.class)
  public void addLeadingZeroTestWithZeroWidth() throws IOException {
    CommonUtils.addLeadingZero(1, -1);
  }

  @Test
  public void getPathWithoutSchemaTest() {
    List<String> schemas = new ArrayList<String>();
    schemas.add("");
    schemas.add("tachyon://abc:19998");
    schemas.add("tachyon-ft://abc:19998");
    schemas.add("tachyon://localhost:19998");
    schemas.add("tachyon-ft://localhost:19998");
    schemas.add("tachyon://127.0.0.1:19998");
    schemas.add("tachyon-ft://127.0.0.1:19998");
    for (int k = 0; k < schemas.size(); k ++) {
      String schema = schemas.get(k);
      if (!schema.equals("")) {
        Assert.assertEquals("/", CommonUtils.getPathWithoutSchema(schema));
      }
      Assert.assertEquals("/", CommonUtils.getPathWithoutSchema(schema + "/"));
      Assert.assertEquals("/123", CommonUtils.getPathWithoutSchema(schema + "/123"));
      Assert.assertEquals("/ab/de.txt", CommonUtils.getPathWithoutSchema(schema + "/ab/de.txt"));
    }
  }

  @Test
  public void parseSpaceSizeTest() {
    long max = 10240;
    for (long k = 0; k < max; k ++) {
      Assert.assertEquals(k / 10, CommonUtils.parseSpaceSize(k / 10.0 + "b"));
      Assert.assertEquals(k / 10, CommonUtils.parseSpaceSize(k / 10.0 + "B"));
      Assert.assertEquals(k / 10, CommonUtils.parseSpaceSize(k / 10.0 + ""));
    }
    for (long k = 0; k < max; k ++) {
      Assert.assertEquals(k * Constants.KB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "kb"));
      Assert.assertEquals(k * Constants.KB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "Kb"));
      Assert.assertEquals(k * Constants.KB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "KB"));
      Assert.assertEquals(k * Constants.KB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "kB"));
    }
    for (long k = 0; k < max; k ++) {
      Assert.assertEquals(k * Constants.MB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "mb"));
      Assert.assertEquals(k * Constants.MB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "Mb"));
      Assert.assertEquals(k * Constants.MB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "MB"));
      Assert.assertEquals(k * Constants.MB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "mB"));
    }
    for (long k = 0; k < max; k ++) {
      Assert.assertEquals(k * Constants.GB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "gb"));
      Assert.assertEquals(k * Constants.GB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "Gb"));
      Assert.assertEquals(k * Constants.GB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "GB"));
      Assert.assertEquals(k * Constants.GB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "gB"));
    }
    for (long k = 0; k < max; k ++) {
      Assert.assertEquals(k * Constants.TB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "tb"));
      Assert.assertEquals(k * Constants.TB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "Tb"));
      Assert.assertEquals(k * Constants.TB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "TB"));
      Assert.assertEquals(k * Constants.TB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "tB"));
    }
    // We stop the pb test before 8192, since 8192 petabytes is beyond the scope of a java long.
    for (long k = 0; k < 8192; k ++) {
      Assert.assertEquals(k * Constants.PB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "pb"));
      Assert.assertEquals(k * Constants.PB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "Pb"));
      Assert.assertEquals(k * Constants.PB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "PB"));
      Assert.assertEquals(k * Constants.PB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "pB"));
    }
  }
}
