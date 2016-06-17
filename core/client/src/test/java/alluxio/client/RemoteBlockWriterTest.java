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
import alluxio.Constants;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

/**
 * Tests for the {@link RemoteBlockWriter} class.
 */
public class RemoteBlockWriterTest {

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Test
  public void createFromMockClass() throws Exception {
    RemoteBlockWriter mock = Mockito.mock(RemoteBlockWriter.class);
    Configuration conf = new Configuration();
    conf.set(Constants.USER_BLOCK_REMOTE_WRITER, mock.getClass().getName());
    Assert.assertTrue(RemoteBlockWriter.Factory.create(conf).getClass().equals(mock.getClass()));
  }

  @Test
  public void createFailed() {
    mThrown.expect(RuntimeException.class);

    Configuration conf = new Configuration();
    conf.set(Constants.USER_BLOCK_REMOTE_WRITER, "unknown");
    RemoteBlockWriter.Factory.create(conf);
  }

}
