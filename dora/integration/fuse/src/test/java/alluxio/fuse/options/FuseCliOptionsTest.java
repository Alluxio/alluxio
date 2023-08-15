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

package alluxio.fuse.options;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import alluxio.AlluxioURI;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.Optional;

public class FuseCliOptionsTest {
  private FuseCliOptions mOptions;
  private JCommander mJCommander;

  @Before
  public void setUp() throws Exception {
    mOptions = new FuseCliOptions();
    mJCommander = JCommander.newBuilder()
        .addObject(mOptions)
        .build();
  }

  @Test
  public void testGetMountPoint() {
    mJCommander.parse("-m", "/tmp/fuse-mp");
    assertEquals(Optional.of(Paths.get("/tmp/fuse-mp")), mOptions.getMountPoint());
  }

  @Test
  public void missingRequiredMountPoint() {
    assertThrows(ParameterException.class, () -> mJCommander.parse("-m"));
    assertThrows(ParameterException.class, () -> mJCommander.parse("-u", "/tmp/fuse-mp"));
  }

  @Test
  public void testGetRootUfsUri() {
    mJCommander.parse("-m", "/tmp/fuse-mp", "-u", "scheme://host/path");
    assertEquals(Optional.of(new AlluxioURI("scheme://host/path")), mOptions.getRootUfsUri());
  }

  @Test
  public void testGetUpdateCheck() {
    mJCommander.parse("-m", "/tmp/fuse-mp", "--update-check");
    assertEquals(Optional.of(true), mOptions.getUpdateCheck());
  }

  @Test
  public void testGetHelp() {
    mJCommander.parse("--help");
    assertEquals(Optional.of(true), mOptions.getHelp());
  }

  @Test
  public void testGetMountOptions() {
    mJCommander.parse("-m", "/tmp/fuse-mp", "-o", "ro", "-o", "key=value");
    assertEquals(Optional.of(new MountOptions(ImmutableMap.of("ro", "", "key", "value"))),
        mOptions.getMountOptions());
  }
}
