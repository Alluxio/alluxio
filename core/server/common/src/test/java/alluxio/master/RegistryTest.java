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

import alluxio.Registry;
import alluxio.Server;

import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

public final class RegistryTest {

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  public abstract class TestServer implements Server<Void> {
    @Override
    @Nullable
    public Map<ServiceType, GrpcService> getServices() {
      return null;
    }

    @Override
    public void start(Void unused) throws IOException {}

    @Override
    public void stop() throws IOException {}

    @Override
    public void close() throws IOException {}
  }

  public class ServerA extends TestServer {
    @Override
    public String getName() {
      return "A";
    }

    @Override
    public Set<Class<? extends Server>> getDependencies() {
      Set<Class<? extends Server>> deps = new HashSet<>();
      deps.add(ServerB.class);
      return deps;
    }
  }

  public class ServerB extends TestServer {
    @Override
    public String getName() {
      return "B";
    }

    @Override
    public Set<Class<? extends Server>> getDependencies() {
      Set<Class<? extends Server>> deps = new HashSet<>();
      deps.add(ServerC.class);
      return deps;
    }
  }

  public class ServerC extends TestServer {
    @Override
    public String getName() {
      return "C";
    }

    @Override
    public Set<Class<? extends Server>> getDependencies() {
      Set<Class<? extends Server>> deps = new HashSet<>();
      deps.add(ServerD.class);
      return deps;
    }
  }

  public class ServerD extends TestServer {
    @Override
    public String getName() {
      return "C";
    }

    @Override
    public Set<Class<? extends Server>> getDependencies() {
      Set<Class<? extends Server>> deps = new HashSet<>();
      deps.add(ServerA.class);
      return deps;
    }
  }

  @Test
  public void registry() {
    List<TestServer> masters = ImmutableList.of(new ServerC(), new ServerB(), new ServerA());
    List<TestServer[]> permutations = new ArrayList<>();
    computePermutations(masters.toArray(new TestServer[masters.size()]), 0, permutations);
    // Make sure that the registry orders the masters independently of the order in which they
    // are registered.
    for (TestServer[] permutation : permutations) {
      Registry<TestServer, Void> registry = new Registry<>();
      for (TestServer server : permutation) {
        registry.add(server.getClass(), server);
      }
      Assert.assertEquals(masters, registry.getServers());
    }
  }

  @Test
  public void cycle() {
    Registry<TestServer, Void> registry = new Registry<>();
    registry.add(ServerA.class, new ServerA());
    registry.add(ServerB.class, new ServerB());
    registry.add(ServerC.class, new ServerC());
    registry.add(ServerC.class, new ServerD());

    mThrown.expect(RuntimeException.class);
    registry.getServers();
  }

  @Test
  public void unavailable() {
    Registry<TestServer, Void> registry = new Registry<>();

    mThrown.expect(Exception.class);
    mThrown.expectMessage("Timed out");
    mThrown.expectMessage("ServerB");
    registry.get(ServerB.class, 100);
  }

  private void computePermutations(TestServer[] input, int index, List<TestServer[]> permutations) {
    if (index == input.length) {
      permutations.add(input.clone());
    }
    for (int i = index; i < input.length; i++) {
      TestServer tmp = input[i];
      input[i] = input[index];
      input[index] = tmp;
      computePermutations(input, index + 1, permutations);
      input[index] = input[i];
      input[i] = tmp;
    }
  }
}
