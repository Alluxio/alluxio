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
import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.PropertyKey.Template;
import alluxio.test.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.TieredIdentity;
import alluxio.wire.TieredIdentity.LocalityTier;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  private InstancedConfiguration mConfiguration;

  @Before
  public void before() {
    mConfiguration = ConfigurationTestUtils.defaults();
  }

  @Test
  public void defaultConf() throws Exception {
    TieredIdentity identity = TieredIdentityFactory.create(mConfiguration);
    TieredIdentity expected = new TieredIdentity(Arrays.asList(
        new LocalityTier("node", NetworkAddressUtils.getLocalNodeName(mConfiguration)),
        new LocalityTier("rack", null)));
    assertEquals(expected, identity);
  }

  @Test
  public void fromScript() throws Exception {
    String scriptPath = setupScript("node=myhost,rack=myrack,custom=mycustom", mFolder.newFile());
    try (Closeable c = new ConfigurationRule(ImmutableMap.of(
        PropertyKey.LOCALITY_ORDER, "node,rack,custom",
        PropertyKey.LOCALITY_SCRIPT, scriptPath), mConfiguration).toResource()) {
      TieredIdentity identity = TieredIdentityFactory.create(mConfiguration);
      TieredIdentity expected = new TieredIdentity(Arrays.asList(
          new LocalityTier("node", "myhost"),
          new LocalityTier("rack", "myrack"),
          new LocalityTier("custom", "mycustom")));
      assertEquals(expected, identity);
    }
  }

  @Test
  public void fromScriptClasspath() throws Exception {
    String customScriptName = "my-alluxio-locality.sh";
    File dir = mFolder.newFolder("fromScriptClasspath");
    CommonUtils.classLoadURL(dir.getCanonicalPath());
    File script = new File(dir, customScriptName);
    setupScript("node=myhost,rack=myrack,custom=mycustom", script);
    try (Closeable c = new ConfigurationRule(ImmutableMap.of(
        PropertyKey.LOCALITY_ORDER, "node,rack,custom",
        PropertyKey.LOCALITY_SCRIPT, customScriptName), mConfiguration).toResource()) {
      TieredIdentity identity = TieredIdentityFactory.create(mConfiguration);
      TieredIdentity expected = new TieredIdentity(Arrays.asList(
          new LocalityTier("node", "myhost"),
          new LocalityTier("rack", "myrack"),
          new LocalityTier("custom", "mycustom")));
      assertEquals(expected, identity);
    }
    script.delete();
  }

  @Test
  public void overrideScript() throws Exception {
    String scriptPath = setupScript("node=myhost,rack=myrack,custom=mycustom", mFolder.newFile());
    try (Closeable c = new ConfigurationRule(ImmutableMap.of(
        Template.LOCALITY_TIER.format("node"), "overridden",
        PropertyKey.LOCALITY_ORDER, "node,rack,custom",
        PropertyKey.LOCALITY_SCRIPT, scriptPath), mConfiguration).toResource()) {
      TieredIdentity identity = TieredIdentityFactory.create(mConfiguration);
      TieredIdentity expected = new TieredIdentity(Arrays.asList(
          new LocalityTier("node", "overridden"),
          new LocalityTier("rack", "myrack"),
          new LocalityTier("custom", "mycustom")));
      assertEquals(expected, identity);
    }
  }

  @Test
  public void outOfOrderScript() throws Exception {
    String scriptPath = setupScript("rack=myrack,node=myhost", mFolder.newFile());
    try (Closeable c = new ConfigurationRule(ImmutableMap.of(
        PropertyKey.LOCALITY_SCRIPT, scriptPath), mConfiguration).toResource()) {
      TieredIdentity identity = TieredIdentityFactory.create(mConfiguration);
      TieredIdentity expected = new TieredIdentity(Arrays.asList(
          new LocalityTier("node", "myhost"),
          new LocalityTier("rack", "myrack")));
      assertEquals(expected, identity);
    }
  }

  @Test
  public void repeatedScriptKey() throws Exception {
    String output = "rack=myrack,node=myhost,rack=myrack2";
    mThrown.expectMessage("Encountered repeated tier definition for rack when parsing "
        + "tiered identity from string rack=myrack,node=myhost,rack=myrack2");
    runScriptWithOutput(output);
  }

  @Test
  public void unknownScriptKey() throws Exception {
    String badOutput = "unknown=x";
    mThrown.expectMessage("Unrecognized tier: unknown. The tiers defined by "
        + "alluxio.locality.order are [node, rack]");
    runScriptWithOutput(badOutput);
  }

  @Test
  public void invalidScriptOutput() throws Exception {
    for (String badOutput : new String[] {"x", "a=b c=d", "node=b,cd", "node=b,abc,x=y"}) {
      try {
        runScriptWithOutput(badOutput);
        fail("Expected an exception to be thrown");
      } catch (Exception e) {
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
        PropertyKey.LOCALITY_SCRIPT, script.getAbsolutePath()), mConfiguration).toResource()) {
      try {
        TieredIdentity identity = TieredIdentityFactory.create(mConfiguration);
      } catch (RuntimeException e) {
        assertThat(e.getMessage(), containsString(script.getAbsolutePath()));
        assertThat(e.getMessage(), containsString("Permission denied"));
      }
    }
  }

  @Test
  public void fromString() throws Exception {
    assertEquals(new TieredIdentity(Arrays.asList(
        new LocalityTier("node", "b"),
        new LocalityTier("rack", "d")
    )), TieredIdentityFactory.fromString("node=b,rack=d", mConfiguration));
  }

  private String setupScript(String tieredIdentityString, File script) throws Exception {
    String content = "#!/bin/bash\n"
        + "echo \"" + tieredIdentityString + "\"\n";
    FileUtils.writeStringToFile(script, content);
    script.setExecutable(true);
    return script.getAbsolutePath();
  }

  private TieredIdentity runScriptWithOutput(String output) throws Exception {
    String scriptPath = setupScript(output, mFolder.newFile());
    try (Closeable c = new ConfigurationRule(ImmutableMap.of(
        PropertyKey.LOCALITY_SCRIPT, scriptPath), mConfiguration).toResource()) {
      return TieredIdentityFactory.create(mConfiguration);
    }
  }
}
