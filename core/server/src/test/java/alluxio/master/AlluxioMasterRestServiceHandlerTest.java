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

package alluxio.master;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.web.MasterUIWebServer;
import alluxio.RuntimeConstants;
import alluxio.master.block.BlockMaster;
import alluxio.master.journal.Journal;
import alluxio.wire.WorkerInfo;

import org.junit.Test;
import org.junit.Before;

import java.util.List;

import javax.ws.rs.core.Response;
import javax.servlet.ServletContext;

/**
 * Unit tests for {@link AlluxioMasterRestServiceHandler}.
 */

public class AlluxioMasterRestServiceHandlerTest {
  private AlluxioMaster mMaster;
  private ServletContext mContext;
  private BlockMaster mBlockMaster;
  private AlluxioMasterRestServiceHandler mHandler;

  @Before
  public void setUp() {
    mMaster = mock(AlluxioMaster.class);
    mContext = mock(ServletContext.class);
    Journal journal = mock(Journal.class);
    mBlockMaster = new BlockMaster(journal);
    when(mMaster.getBlockMaster()).thenReturn(mBlockMaster);
    when(mContext.getAttribute(MasterUIWebServer.ALLUXIO_MASTER_SERVLET_RESOURCE_KEY)).thenReturn(
        mMaster);
    mHandler = new AlluxioMasterRestServiceHandler(mContext);
  }

  @Test
  public void testGetVersion() {
    Response response = mHandler.getVersion();
    assertNotNull("Response must be not null!", response);
    assertNotNull("Response must have a entry!", response.getEntity());
    assertEquals("Entry must be a String!", String.class, response.getEntity().getClass());
    String entry = (String) response.getEntity();
    assertEquals("\"" + RuntimeConstants.VERSION + "\"", entry);
  }

  @Test
  public void testGetWorkerCount() {
    Response response = mHandler.getWorkerCount();
    assertNotNull("Response must be not null!", response);
    assertNotNull("Response must have a entry!", response.getEntity());
    assertEquals("Entry must be a Integer!", Integer.class, response.getEntity().getClass());
    Integer entry = (Integer) response.getEntity();
    assertEquals(Integer.valueOf(0), entry);
  }

  @Test
  public void testGetWorkerInfoList() {
    Response response = mHandler.getWorkerInfoList();
    assertNotNull("Response must be not null!", response);
    assertNotNull("Response must have a entry!", response.getEntity());
    assertTrue("Entry must be a List!", (response.getEntity() instanceof List));
    List<WorkerInfo> entry = (List<WorkerInfo>) response.getEntity();
    assertTrue(entry.isEmpty());
  }
}
