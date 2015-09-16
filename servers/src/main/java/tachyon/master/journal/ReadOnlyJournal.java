/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.journal;

import tachyon.conf.TachyonConf;

/**
 * The read-only version of {@link Journal}. It prevents access to a {@link JournalWriter}.
 */
public class ReadOnlyJournal extends Journal {
  /**
   * @param directory the base directory for the journal
   * @param tachyonConf the tachyon conf
   */
  public ReadOnlyJournal(String directory, TachyonConf tachyonConf) {
    super(directory, tachyonConf);
  }

  @Override
  public JournalWriter getNewWriter() {
    throw new IllegalStateException("Cannot get a writer for a read-only journal.");
  }
}
