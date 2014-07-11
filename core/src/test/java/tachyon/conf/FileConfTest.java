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
package tachyon.conf;

import java.io.IOException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.conf.CommonConf;

/**
 * Unit tests for <code>tachyon.conf.FileConfTest</code>.
 */
public class FileConfTest {
  private final String mConfFile = "/tmp/tachyon-conf-test.conf";
  private final String mIntProperty = "tachyon.conf.test.int";
  private final int    mIntValue = 99;
  private final String mStringProperty = "tachyon.conf.test.string";
  private final String mStringValue = "junk";

  private final String mStringPropertyOverwrite = "tachyon.conf.test.overwrite.string";
  private final String mStringValueFile = "File";
  private final String mStringValueSys = "Cmd";

  private void writeConf() throws IOException {
    File conf = new File(mConfFile);
    FileOutputStream is = new FileOutputStream(conf);
    OutputStreamWriter wr = new OutputStreamWriter(is);
    Writer w = new BufferedWriter(wr); 
    w.write(mIntProperty + "=" + mIntValue + "\n");
    w.write(mStringProperty + "=" + mStringValue + "\n");
    w.write(mStringPropertyOverwrite + "=" + mStringValueFile + "\n");
    w.close();
    conf.deleteOnExit();
  }
  @After
  public final void after() throws Exception {
    System.clearProperty(mStringPropertyOverwrite);
    CommonConf.clear();
  }

  @Before
  public final void before() throws IOException {
    writeConf();
    System.setProperty(mStringPropertyOverwrite, mStringValueSys);
  }

  @Test
  public void GetConfProperty() throws Exception {
    CommonConf.clear();
    // test constructor
    Assert.assertNotNull(CommonConf.get(mConfFile));
    // test get int
    Assert.assertEquals(CommonConf.get(mConfFile).getIntProperty(mIntProperty), mIntValue);
    // test get string
    Assert.assertEquals(CommonConf.get(mConfFile).getProperty(mStringProperty), mStringValue);
    // test overwrite order sys > file
    Assert.assertEquals(CommonConf.get(mConfFile).getProperty(mStringPropertyOverwrite), mStringValueSys);
  }
}
