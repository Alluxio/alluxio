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

package tachyon.master.next.journal;

import java.io.IOException;
import java.io.OutputStream;

import tachyon.conf.TachyonConf;
import tachyon.master.next.filesystem.journal.EventReadWriter;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.io.PathUtils;

// TODO(cc)
public class EventLogManager {
  private EventReadWriter mReadWriter;
  private OutputStream mOs;

  public EventLogManager(String folder, String name, EventReadWriter readWriter, TachyonConf conf)
      throws IOException {
    mReadWriter = readWriter;
    UnderFileSystem ufs = UnderFileSystem.get(folder, conf);
    // TODO(cc) connect to ufs, clean paths...
    mOs = ufs.create(PathUtils.concatPath(folder, name));
    // TODO(cc)
  }

  public void writeEvent(Event event) {
    try {
      mReadWriter.writeEvent(event, mOs);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}
