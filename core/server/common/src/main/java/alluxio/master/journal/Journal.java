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

import java.net.URL;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class encapsulates the journal for a master. The journal is made up of 2 components:
 * - The checkpoint: the full state of the master
 * - The entries: incremental entries to apply to the checkpoint.
 *
 * To construct the full state of the master, all the entries must be applied to the checkpoint in
 * order. The entry file most recently being written to is in the base journal folder, where the
 * completed entry files are in the "completed/" sub-directory.
 */
@ThreadSafe
public interface Journal {

  /**
   * @return the location for this journal
   */
  URL getLocation();
}
