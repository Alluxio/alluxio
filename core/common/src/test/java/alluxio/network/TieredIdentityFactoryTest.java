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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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
  public void repeatedScript() throws Exception {
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
  public void invalidScriptOutput() throws Exception {
    assertNull(TieredIdentityFactory.parseIdentityString("x"));
    assertNull(TieredIdentityFactory.parseIdentityString("a=b c=d"));
    assertNull(TieredIdentityFactory.parseIdentityString("a=b,cd"));
    assertNull(TieredIdentityFactory.parseIdentityString("a=b,abc,x=y"));
  }

  private String setupScript(String tieredIdentityString) throws Exception {
    File script = mFolder.newFile("locality-script");
    script.setExecutable(true);
    String content = "#!/bin/bash\n"
        + "echo \"" + tieredIdentityString + "\"\n";
    FileUtils.writeStringToFile(script, content);
    return script.getAbsolutePath();
  }
}
