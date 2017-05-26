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

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.ServiceUtils;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalFactory;
import alluxio.util.CommonUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * This class encapsulates the different master services that are configured to run.
 */
final class MasterUtils {
  private MasterUtils() {}  // prevent instantiation

  /**
   * Checks whether the journal has been formatted.
   */
  public static void checkJournalFormatted() throws IOException {
    Journal.Factory factory = new Journal.Factory(getJournalLocation());
    for (String name : ServiceUtils.getMasterServiceNames()) {
      Journal journal = factory.create(name);
      if (!journal.isFormatted()) {
        throw new RuntimeException(
            String.format("Journal %s has not been formatted!", journal.getLocation()));
      }
    }
  }

  /**
   * @return the journal location
   */
  public static URI getJournalLocation() {
    String journalDirectory = Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
    if (!journalDirectory.endsWith(AlluxioURI.SEPARATOR)) {
      journalDirectory += AlluxioURI.SEPARATOR;
    }
    try {
      return new URI(journalDirectory);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates all the masters and registers them to the master registry.
   *
   * @param journalFactory the factory to use for creating journals
   * @param registry the master registry
   */
  public static void createMasters(final JournalFactory journalFactory,
      final MasterRegistry registry) {
    List<Callable<Void>> callables = new ArrayList<>();
    for (final MasterFactory factory : ServiceUtils.getMasterServiceLoader()) {
      callables.add(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          if (factory.isEnabled()) {
            factory.create(registry, journalFactory);
          }
          return null;
        }
      });
    }
    try {
      CommonUtils.invokeAll(callables, 10, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new RuntimeException("Failed to start masters", e);
    }
  }
}
