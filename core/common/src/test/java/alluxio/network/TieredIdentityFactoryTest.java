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

package alluxio.network;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import alluxio.ConfigurationRule;
import alluxio.PropertyKey;
import alluxio.PropertyKey.Template;
import alluxio.wire.TieredIdentity;
import alluxio.wire.TieredIdentity.LocalityTier;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.Closeable;
import java.io.File;
import java.util.Arrays;

/**
 * Unit tests for {@link TieredIdentityFactory}.
 */
public class TieredIdentityFactoryTest {
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Test
  public void defaultConf() throws Exception {
    TieredIdentity identity = TieredIdentityFactory.create();
    TieredIdentity expected = new TieredIdentity(Arrays.asList(
        new LocalityTier("node", null),
        new LocalityTier("rack", null)));
    assertEquals(expected, identity);
  }

  @Test
  public void fromScript() throws Exception {
    String scriptPath = setupScript("node=myhost,rack=myrack,custom=mycustom");
    try (Closeable c = new ConfigurationRule(ImmutableMap.of(
        PropertyKey.LOCALITY_ORDER, "node,rack,custom",
        PropertyKey.LOCALITY_SCRIPT, scriptPath)).toResource()) {
      TieredIdentity identity = TieredIdentityFactory.create();
      TieredIdentity expected = new TieredIdentity(Arrays.asList(
          new LocalityTier("node", "myhost"),
          new LocalityTier("rack", "myrack"),
          new LocalityTier("custom", "mycustom")));
      assertEquals(expected, identity);
    }
  }

  @Test
  public void overrideScript() throws Exception {
    String scriptPath = setupScript("node=myhost,rack=myrack,custom=mycustom");
    try (Closeable c = new ConfigurationRule(ImmutableMap.of(
        Template.LOCALITY_TIER.format("node"), "overridden",
        PropertyKey.LOCALITY_ORDER, "node,rack,custom",
        PropertyKey.LOCALITY_SCRIPT, scriptPath)).toResource()) {
      TieredIdentity identity = TieredIdentityFactory.create();
      TieredIdentity expected = new TieredIdentity(Arrays.asList(
          new LocalityTier("node", "overridden"),
          new LocalityTier("rack", "myrack"),
          new LocalityTier("custom", "mycustom")));
      assertEquals(expected, identity);
    }
  }

  @Test
  public void outOfOrderScript() throws Exception {
    String scriptPath = setupScript("rack=myrack,node=myhost");
    try (Closeable c = new ConfigurationRule(ImmutableMap.of(
        PropertyKey.LOCALITY_SCRIPT, scriptPath)).toResource()) {
      TieredIdentity identity = TieredIdentityFactory.create();
      TieredIdentity expected = new TieredIdentity(Arrays.asList(
          new LocalityTier("node", "myhost"),
          new LocalityTier("rack", "myrack")));
      assertEquals(expected, identity);
    }
  }

  @Test
  public void repeatedScriptKey() throws Exception {
    String output = "rack=myrack,node=myhost,rack=myrack2";
    try {
      runScriptWithOutput(output);
      fail("Expected an exception to be thrown");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), containsString(output));
      assertThat(e.getMessage(), containsString("repeated tier definition"));
    }
  }

  @Test
  public void invalidScriptOutput() throws Exception {
    for (String badOutput : new String[] {"x", "a=b c=d", "a=b,cd", "a=b,abc,x=y"}) {
      try {
        runScriptWithOutput(badOutput);
        fail("Expected an exception to be thrown");
      } catch (RuntimeException e) {
        assertThat(e.getMessage(), containsString("Failed to parse"));
        assertThat(e.getMessage(), containsString(badOutput));
      }
    }
  }

  @Test
  public void notExecutable() throws Exception {
    File script = mFolder.newFile();
    FileUtils.writeStringToFile(script, "#!/bin/bash");
    try (Closeable c = new ConfigurationRule(ImmutableMap.of(
        PropertyKey.LOCALITY_SCRIPT, script.getAbsolutePath())).toResource()) {
      try {
        TieredIdentity identity = TieredIdentityFactory.create();
      } catch (RuntimeException e) {
        assertThat(e.getMessage(), containsString(script.getAbsolutePath()));
        assertThat(e.getMessage(), containsString("Permission denied"));
      }
    }
  }

  private String setupScript(String tieredIdentityString) throws Exception {
    File script = mFolder.newFile();
    script.setExecutable(true);
    String content = "#!/bin/bash\n"
        + "echo \"" + tieredIdentityString + "\"\n";
    FileUtils.writeStringToFile(script, content);
    return script.getAbsolutePath();
  }

  private TieredIdentity runScriptWithOutput(String output) throws Exception {
    String scriptPath = setupScript(output);
    try (Closeable c = new ConfigurationRule(ImmutableMap.of(
        PropertyKey.LOCALITY_SCRIPT, scriptPath)).toResource()) {
      return TieredIdentityFactory.create();
    }
  }
}
