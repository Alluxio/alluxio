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

import alluxio.uri.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Utility methods for working with the Alluxio journal.
 */
public final class JournalUtils {

  /**
   * Returns a URI for the configured location for the specified journal.
   *
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

  private JournalUtils() {} // prevent instantiation
}
