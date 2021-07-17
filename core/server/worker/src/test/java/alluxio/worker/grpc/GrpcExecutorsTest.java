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

package alluxio.worker.grpc;

import static org.junit.Assert.assertEquals;

import alluxio.security.User;
import alluxio.security.authentication.AuthenticatedClientUser;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;

public class GrpcExecutorsTest {
  private static final String IMPERSONATION_PROXY_USER_NAME = "foo";

  @Before
  public void before() {
    AuthenticatedClientUser.remove();
  }

  @After
  public void after() {
    AuthenticatedClientUser.remove();
  }

  private void validateAuthenticatedClientUser(ExecutorService executor) {
    final User contextProxyUser = AuthenticatedClientUser.getOrNull();
    executor.execute(() -> {
      User workerProxyUser = AuthenticatedClientUser.getOrNull();
      assertEquals(contextProxyUser, workerProxyUser);
    });

    AuthenticatedClientUser.set(IMPERSONATION_PROXY_USER_NAME);
    User newContextProxyUser = AuthenticatedClientUser.getOrNull();
    executor.execute(() -> {
      User workerProxyUser = AuthenticatedClientUser.getOrNull();
      assertEquals(newContextProxyUser, workerProxyUser);
    });
  }

  @Test
  public void impersonationPassedToBlockReader() {
    validateAuthenticatedClientUser(GrpcExecutors.BLOCK_READER_EXECUTOR);
  }

  @Test
  public void impersonationPassedToBlockWriter() {
    validateAuthenticatedClientUser(GrpcExecutors.BLOCK_WRITER_EXECUTOR);
  }

  @Test
  public void impersonationPassedToAsyncCacheManager() {
    validateAuthenticatedClientUser(GrpcExecutors.CACHE_MANAGER_EXECUTOR);
  }
}
