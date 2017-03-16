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

import org.apache.thrift.TProcessor;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
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
    String[] expectedNames = {"C", "B", "A"};

    {
      MasterRegistry registry = new MasterRegistry();
      registry.add(MasterA.class, new MasterA());
      registry.add(MasterB.class, new MasterB());
      registry.add(MasterC.class, new MasterC());

      int i = 0;
      for (Master master : registry.getMasters()) {
        Assert.assertEquals(master.getName(), expectedNames[i++]);
      }
    }

    {
      MasterRegistry registry = new MasterRegistry();
      registry.add(MasterA.class, new MasterA());
      registry.add(MasterC.class, new MasterC());
      registry.add(MasterB.class, new MasterB());

      int i = 0;
      for (Master master : registry.getMasters()) {
        Assert.assertEquals(master.getName(), expectedNames[i++]);
      }
    }

    {
      MasterRegistry registry = new MasterRegistry();
      registry.add(MasterB.class, new MasterB());
      registry.add(MasterA.class, new MasterA());
      registry.add(MasterC.class, new MasterC());

      int i = 0;
      for (Master master : registry.getMasters()) {
        Assert.assertEquals(master.getName(), expectedNames[i++]);
      }
    }

    {
      MasterRegistry registry = new MasterRegistry();
      registry.add(MasterB.class, new MasterB());
      registry.add(MasterC.class, new MasterC());
      registry.add(MasterA.class, new MasterA());

      int i = 0;
      for (Master master : registry.getMasters()) {
        Assert.assertEquals(master.getName(), expectedNames[i++]);
      }
    }

    {
      MasterRegistry registry = new MasterRegistry();
      registry.add(MasterC.class, new MasterC());
      registry.add(MasterA.class, new MasterA());
      registry.add(MasterB.class, new MasterB());

      int i = 0;
      for (Master master : registry.getMasters()) {
        Assert.assertEquals(master.getName(), expectedNames[i++]);
      }
    }

    {
      MasterRegistry registry = new MasterRegistry();
      registry.add(MasterC.class, new MasterC());
      registry.add(MasterB.class, new MasterB());
      registry.add(MasterA.class, new MasterA());

      int i = 0;
      for (Master master : registry.getMasters()) {
        Assert.assertEquals(master.getName(), expectedNames[i++]);
      }
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
}
