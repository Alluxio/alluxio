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

package alluxio.client;

import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;

import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

public class RemoteBlockReaderTest {
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @After
  public void after() {
    ConfigurationTestUtils.resetConfiguration();
    RemoteBlockReader.Factory.sReader = null;
  }

  @Test
  public void createFromMockClass() {
    RemoteBlockReader mock = Mockito.mock(RemoteBlockReader.class);
    Configuration.set(PropertyKey.USER_BLOCK_REMOTE_READER_CLASS, mock.getClass().getName());
    Assert.assertTrue(RemoteBlockReader.Factory.create().getClass().equals(mock.getClass()));
  }

  @Test
  public void createFailed() {
    Configuration.set(PropertyKey.USER_BLOCK_REMOTE_READER_CLASS, "unknown");
    mThrown.expect(RuntimeException.class);
    RemoteBlockReader.Factory.create();
  }

  @Test
  public void createIfNotExistFromMockClass() {
    RemoteBlockReader mock = Mockito.mock(RemoteBlockReader.class);
    Configuration.set(PropertyKey.USER_BLOCK_REMOTE_READER_CLASS, mock.getClass().getName());
    Assert.assertTrue(RemoteBlockReader.Factory.createIfNotExist().getClass()
        .equals(mock.getClass()));
  }

  @Test
  public void createIfNotExistFailed() {
    Configuration.set(PropertyKey.USER_BLOCK_REMOTE_READER_CLASS, "unknown");
    mThrown.expect(RuntimeException.class);
    RemoteBlockReader.Factory.createIfNotExist();
  }

  @Test
  public void createIfNotExistNoNewInstance() {
    Assert.assertNull(RemoteBlockReader.Factory.sReader);

    RemoteBlockReader reader = RemoteBlockReader.Factory.createIfNotExist();

    Assert.assertTrue(reader.equals(RemoteBlockReader.Factory.sReader));

    RemoteBlockReader reader1 = RemoteBlockReader.Factory.createIfNotExist();

    Assert.assertTrue(reader1 == reader);
  }
}
