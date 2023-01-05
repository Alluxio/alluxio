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

package alluxio.master.journal;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.PortRegistry;
import alluxio.util.CommonUtils.ProcessType;

import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility methods for testing against a journal system.
 */
public class JournalTestUtils {

  public static List<Integer> createEmbeddedJournalTestPorts(int count) throws IOException {
    List<Integer> ports = new ArrayList<>();
    StringBuilder addresses = new StringBuilder();
    for (int i = 0; i < count; i++) {
      if (i != 0) {
        addresses.append(",");
      }
      int port = PortRegistry.getFreePort();
      ports.add(port);
      addresses.append(String.format("localhost:%d", port));
    }
    Configuration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES, addresses.toString());
    Configuration.set(PropertyKey.MASTER_HOSTNAME, "localhost");
    Configuration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_PORT, ports.get(0));
    return ports;
  }

  public static JournalSystem createJournalSystem(TemporaryFolder folder) {
    try {
      return createJournalSystem(folder.newFolder("journal").getAbsolutePath());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static JournalSystem createJournalSystem(String folder) {
    try {
      return new JournalSystem.Builder()
          .setLocation(new URI(folder))
          .setQuietTimeMs(0)
          .build(ProcessType.MASTER);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}
