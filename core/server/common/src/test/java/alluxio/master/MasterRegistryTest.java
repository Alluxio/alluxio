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

import alluxio.exception.ExceptionMessage;
import alluxio.master.journal.JournalInputStream;
import alluxio.master.journal.JournalOutputStream;
import alluxio.proto.journal.Journal;

import com.google.common.collect.ImmutableList;
import org.apache.thrift.TProcessor;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MasterRegistryTest {

  public abstract class TestMaster implements Master {
    @Override
    public Map<String, TProcessor> getServices() {
      return null;
    }

    @Override
    public void processJournalCheckpoint(JournalInputStream inputStream) throws IOException {
      return;
    }

    @Override
    public void processJournalEntry(Journal.JournalEntry entry) throws IOException {
      return;
    }

    @Override
    public void start(boolean isLeader) throws IOException {
      return;
    }

    @Override
    public void stop() throws IOException {
      return;
    }

    @Override
    public void streamToJournalCheckpoint(JournalOutputStream outputStream) throws IOException {
      return;
    }

    @Override
    public void transitionToLeader() {
      return;
    }
  }

  public class MasterA extends TestMaster {
    @Override
    public String getName() {
      return "A";
    }

    @Override
    public Set<Class<?>> getDependencies() {
      Set<Class<?>> deps = new HashSet<>();
      deps.add(MasterB.class);
      return deps;
    }
  }

  public class MasterB extends TestMaster {
    @Override
    public String getName() {
      return "B";
    }

    @Override
    public Set<Class<?>> getDependencies() {
      Set<Class<?>> deps = new HashSet<>();
      deps.add(MasterC.class);
      return deps;
    }
  }

  public class MasterC extends TestMaster {
    @Override
    public String getName() {
      return "C";
    }

    @Override
    public Set<Class<?>> getDependencies() {
      Set<Class<?>> deps = new HashSet<>();
      deps.add(MasterD.class);
      return deps;
    }
  }

  public class MasterD extends TestMaster {
    @Override
    public String getName() {
      return "C";
    }

    @Override
    public Set<Class<?>> getDependencies() {
      Set<Class<?>> deps = new HashSet<>();
      deps.add(MasterA.class);
      return deps;
    }
  }

  @Test
  public void registry() {
    List<Master> masters = ImmutableList.<Master>of(new MasterC(), new MasterB(), new MasterA());
    List<Master[]> permutations = new ArrayList<>();
    computePermutations(masters.toArray(new Master[masters.size()]), 0, permutations);
    // Make sure that the registry orders the masters independently of the order in which they
    // are registered.
    for (Master[] permutation : permutations) {
      MasterRegistry registry = new MasterRegistry();
      for (int i = 0; i < permutation.length; i++) {
        registry.add(permutation[i].getClass(), permutation[i]);
      }
      Assert.assertEquals(masters, registry.getMasters());
    }
  }

  @Test
  public void cycle() {
    MasterRegistry registry = new MasterRegistry();
    registry.add(MasterA.class, new MasterA());
    registry.add(MasterB.class, new MasterB());
    registry.add(MasterC.class, new MasterC());
    registry.add(MasterC.class, new MasterD());
    try {
      registry.getMasters();
      Assert.fail("Control flow should not reach here.");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), ExceptionMessage.DEPENDENCY_CYCLE.getMessage());
    }
  }

  @Test
  public void unavailable() {
    MasterRegistry registry = new MasterRegistry();
    try {
      registry.get(MasterB.class);
      Assert.fail("Control flow should not reach here.");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), ExceptionMessage.RESOURCE_UNAVAILABLE.getMessage());
    }
  }

  private void computePermutations(Master[] input, int index, List<Master[]> permutations) {
    if (index == input.length) {
      permutations.add(input.clone());
    }
    for (int i = index; i < input.length; i++) {
      Master tmp = input[i];
      input[i] = input[index];
      input[index] = tmp;
      computePermutations(input, index + 1, permutations);
      input[index] = input[i];
      input[i] = tmp;
    }
  }
}
