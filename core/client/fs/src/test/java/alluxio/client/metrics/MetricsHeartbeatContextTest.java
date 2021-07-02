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

package alluxio.client.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.ClientContext;
import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.MasterInquireClient;

import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

public class MetricsHeartbeatContextTest {

  @Test
  public void testExecutorInitialized() {
    InstancedConfiguration conf = ConfigurationTestUtils.defaults();
    conf.set(PropertyKey.MASTER_HOSTNAME, "localhost");
    conf.set(PropertyKey.USER_RPC_RETRY_MAX_DURATION, "1s");
    ClientContext ctx = ClientContext.create(conf);
    MasterInquireClient client = MasterInquireClient.Factory
        .create(ctx.getClusterConf(), ctx.getUserState());

    // Add and delete a single context, make sure it is non null after adding, and then null after
    // removing
    MetricsHeartbeatContext.addHeartbeat(ctx, client);
    assertNotNull(getInternalExecutor());
    MetricsHeartbeatContext.removeHeartbeat(ctx);
    assertNull(getInternalExecutor());

    // Add a few, then remove and check for the state in between
    MetricsHeartbeatContext.addHeartbeat(ctx, client);
    MetricsHeartbeatContext.addHeartbeat(ctx, client);
    MetricsHeartbeatContext.addHeartbeat(ctx, client);
    MetricsHeartbeatContext.addHeartbeat(ctx, client);
    assertNotNull(getInternalExecutor());
    MetricsHeartbeatContext.removeHeartbeat(ctx);
    assertNotNull(getInternalExecutor());
    MetricsHeartbeatContext.removeHeartbeat(ctx);
    assertNotNull(getInternalExecutor());
    MetricsHeartbeatContext.removeHeartbeat(ctx);
    assertNotNull(getInternalExecutor());
    MetricsHeartbeatContext.removeHeartbeat(ctx);
    assertNull(getInternalExecutor());
  }

  @Test
  public void testContextCounter() {
    Map<MasterInquireClient.ConnectDetails, MetricsHeartbeatContext> map =
        getContextMap();
    assertTrue(map.isEmpty());

    InstancedConfiguration conf = ConfigurationTestUtils.defaults();
    conf.set(PropertyKey.USER_RPC_RETRY_MAX_DURATION, "1s");
    ClientContext ctx = ClientContext.create(conf);
    MasterInquireClient client = MasterInquireClient.Factory
        .create(ctx.getClusterConf(), ctx.getUserState());
    MetricsHeartbeatContext.addHeartbeat(ctx, client);
    assertFalse(map.isEmpty());

    map.forEach((details, context) ->
        assertEquals(1, (int) Whitebox.getInternalState(context, "mCtxCount")));

    MetricsHeartbeatContext.addHeartbeat(ctx, client);
    map.forEach((details, context) ->
        assertEquals(2, (int) Whitebox.getInternalState(context, "mCtxCount")));

    conf = ConfigurationTestUtils.defaults();
    conf.set(PropertyKey.USER_RPC_RETRY_MAX_DURATION, "1s");
    conf.set(PropertyKey.MASTER_RPC_ADDRESSES, "master1:19998,master2:19998,master3:19998");
    ClientContext haCtx = ClientContext.create(conf);
    MetricsHeartbeatContext.addHeartbeat(haCtx, MasterInquireClient.Factory
        .create(conf, haCtx.getUserState()));
    assertEquals(2, map.size());

    MetricsHeartbeatContext.removeHeartbeat(ctx);
    assertEquals(2, map.size());
    map.forEach((details, context) ->
        assertEquals(1, (int) Whitebox.getInternalState(context, "mCtxCount")));
    assertNotNull(getInternalExecutor());

    MetricsHeartbeatContext.removeHeartbeat(ctx);
    MetricsHeartbeatContext.removeHeartbeat(haCtx);
    assertNull(getInternalExecutor());
    assertTrue(map.isEmpty());
  }

  @Test
  public void testCancelFuture() {
    Map<MasterInquireClient.ConnectDetails, MetricsHeartbeatContext> map =
        getContextMap();
    assertTrue(map.isEmpty());

    ScheduledFuture<?> future = Mockito.mock(ScheduledFuture.class);
    when(future.cancel(any(Boolean.class))).thenReturn(true);
    InstancedConfiguration conf = ConfigurationTestUtils.defaults();
    conf.set(PropertyKey.USER_RPC_RETRY_MAX_DURATION, "1s");
    ClientContext ctx = ClientContext.create(conf);
    MasterInquireClient client = MasterInquireClient.Factory
        .create(ctx.getClusterConf(), ctx.getUserState());
    MetricsHeartbeatContext.addHeartbeat(ctx, client);
    assertFalse(map.isEmpty());
    map.forEach((addr, heartbeat) -> {
      ScheduledFuture<?> realFuture =
          Whitebox.getInternalState(heartbeat, "mMetricsMasterHeartbeatTask");
      assertNotNull(realFuture); // Should be created and scheduled
      assertFalse(realFuture.isDone()); // Scheduled indefinitely
      Whitebox.setInternalState(heartbeat, "mMetricsMasterHeartbeatTask", future);
      realFuture.cancel(false); // Cancel the real one once replaced with a mock.
    });

    MetricsHeartbeatContext.removeHeartbeat(ctx);
    verify(future).cancel(false); // Make sure the future is canceled afterwards
  }

  private ScheduledExecutorService getInternalExecutor() {
    return getSingleStaticField(ScheduledExecutorService.class);
  }

  private Map<MasterInquireClient.ConnectDetails, MetricsHeartbeatContext> getContextMap() {
    return getSingleStaticField(Map.class);
  }

  private <T> T getSingleStaticField(Class<T> clazz) {
    Set<Field> fields = Whitebox.getAllStaticFields(MetricsHeartbeatContext.class);
    for (Field f : fields) {
      if (f.getType() == clazz) {
        f.setAccessible(true);
        try {
          return clazz.cast(f.get(null));
        } catch (IllegalAccessException e) {
          fail();
        }
      }
    }
    fail("Couldn't find static field of type: " + clazz);
    return null;
  }
}
