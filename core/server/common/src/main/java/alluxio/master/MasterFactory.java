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

import alluxio.master.journal.JournalFactory;

/**
 * Interface for factory of {@link Master}.
 */
public interface MasterFactory {
  /**
   * @return whether the master is enabled
   */
  boolean isEnabled();

  /**
   * @return the master's name
   */
  String getName();

  /**
   * Factory method to create a new master instance.
   *
   * @param registry the master registry
   * @param factory a factory for creating the journal
   *
   * @return a new {@link Master} instance or null if the master is not enabled
   */
  Master create(MasterRegistry registry, JournalFactory factory);
}
