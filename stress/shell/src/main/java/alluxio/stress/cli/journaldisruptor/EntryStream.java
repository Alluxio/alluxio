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

package alluxio.stress.cli.journaldisruptor;

import alluxio.proto.journal.Journal;

import java.io.Closeable;
import java.io.IOException;

/**
 * the abstract class of EntryStream, can be implemented for both Ufs Journal and Raft Journal.
 * Has two method, nextEntry will return next Alluxio entry
 * null when the stream comes the end. Close is decoration, do nothing now.
 */
public abstract class EntryStream implements Closeable {

  protected final String mMaster;
  protected final long mStart;
  protected final long mEnd;
  protected final String mInputDir;

  /**
   * init EntryStream.
   * @param master
   * @param start
   * @param end
   * @param inputDir
   */
  public EntryStream(String master, long start, long end, String inputDir) {
    mMaster = master;
    mStart = start;
    mEnd = end;
    mInputDir = inputDir;
  }

  /**
   * return one journal entry and one step forward.
   * @return next Alluxio journal entry
   */
  public abstract Journal.JournalEntry nextEntry();

  @Override
  public void close() throws IOException {}
}
