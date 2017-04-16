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

package alluxio.master.file.options;

import alluxio.CommonTestUtils;
import alluxio.proto.journal.File;
import alluxio.thrift.MountTOptions;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link MountOptions}.
 */
public final class MountOptionsTest {
  public static final String PROPERTY_KEY = "A";
  public static final String PROPERTY_VALUE = "B";

  /**
   * Tests the {@link MountOptions#defaults()} method.
   */
  @Test
  public void defaults() {
    MountOptions options = MountOptions.defaults();
    Assert.assertFalse(options.isReadOnly());
    Assert.assertTrue(options.getProperties().isEmpty());
    Assert.assertFalse(options.isShared());
  }

  /**
   * Tests creating a {@link MountOptions} from a thrift object.
   */
  @Test
  public void FromThrift() {
    // Null thrift options
    MountTOptions thriftOptions = null;
    MountOptions options = new MountOptions(thriftOptions);
    Assert.assertFalse(options.isReadOnly());
    Assert.assertFalse(options.isShared());

    // Default thrift options
    thriftOptions = new MountTOptions();
    options = new MountOptions(thriftOptions);
    Assert.assertFalse(options.isReadOnly());
    Assert.assertFalse(options.isShared());

    // Set thrift options
    Map<String, String> properties = new HashMap<>();
    properties.put(PROPERTY_KEY, PROPERTY_VALUE);
    thriftOptions = new MountTOptions();
    thriftOptions.setReadOnly(true);
    thriftOptions.setShared(true);
    thriftOptions.setProperties(properties);
    options = new MountOptions(thriftOptions);
    Assert.assertTrue(options.isReadOnly());
    Assert.assertTrue(options.isShared());
    Assert.assertEquals(properties.size(), options.getProperties().size());
    Assert.assertEquals(PROPERTY_VALUE, options.getProperties().get(PROPERTY_KEY));
  }

  /**
   * Tests creating a {@link MountOptions} from a proto object.
   */
  @Test
  public void FromProto() {
    // Null proto options
    File.AddMountPointEntry protoOptions = null;
    MountOptions options = new MountOptions(protoOptions);
    Assert.assertFalse(options.isReadOnly());
    Assert.assertFalse(options.isShared());

    // Default proto options
    protoOptions = File.AddMountPointEntry.newBuilder().build();
    options = new MountOptions(protoOptions);
    Assert.assertFalse(options.isReadOnly());

    // Set proto options
    List<File.StringPairEntry> protoProperties = new ArrayList<>();
    protoProperties.add(File.StringPairEntry.newBuilder()
        .setKey(PROPERTY_KEY)
        .setValue(PROPERTY_VALUE)
        .build());
    protoOptions =
        File.AddMountPointEntry.newBuilder().setReadOnly(true).addAllProperties(protoProperties)
            .setShared(true).build();
    options = new MountOptions(protoOptions);
    Assert.assertTrue(options.isReadOnly());
    Assert.assertTrue(options.isShared());
    Assert.assertEquals(protoProperties.size(), options.getProperties().size());
    Assert.assertEquals(PROPERTY_VALUE, options.getProperties().get(PROPERTY_KEY));
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    MountOptions options = MountOptions.defaults().setReadOnly(true);
    Assert.assertTrue(options.isReadOnly());

    options = MountOptions.defaults().setReadOnly(false);
    Assert.assertFalse(options.isReadOnly());

    options = MountOptions.defaults().setShared(true);
    Assert.assertTrue(options.isShared());
    options = MountOptions.defaults().setShared(false);
    Assert.assertFalse(options.isShared());

    Map<String, String> properties = new HashMap<>();
    properties.put(PROPERTY_KEY, PROPERTY_VALUE);
    options = MountOptions.defaults().setProperties(properties);
    Assert.assertEquals(properties.size(), options.getProperties().size());
    Assert.assertEquals(PROPERTY_VALUE, options.getProperties().get(PROPERTY_KEY));
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(MountOptions.class);
  }
}
