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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.master.Master;
import tachyon.util.CommonUtils;

public class JournalTailerThread extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final Master mMaster;
  private final Journal mJournal;
  /** This become true when this class is instructed to shutdown. */
  private boolean mInitiateShutdown = false;

  public JournalTailerThread(Master master, Journal journal) {
    mMaster = master;
    mJournal = journal;
  }

  public void shutdown() {
    LOG.info(mMaster.getProcessorName() + " Journal tailer shutdown has been initiated.");
    mInitiateShutdown = true;
  }

  public void shutdownAndJoin() {
    shutdown();
    try {
      // Wait for the thread to finish.
      join();
    } catch (InterruptedException ie) {
      LOG.warn("stopping the journal tailer caused exception: " + ie.getMessage());
    }
  }

  @Override
  public void run() {
    LOG.info(mMaster.getProcessorName() + " Journal tailer started.");
    // Continually loop loading the checkpoint file, and then loading all completed files. The loop
    // only repeats when the checkpoint file is updated after it was read.
    while (true) {
      try {
        // Load the checkpoint file.
        JournalReader reader = mJournal.getNewReader();
        mMaster.processJournalCheckpoint(reader.getCheckpointInputStream());

        // The start time (ms) for the initiated shutdown.
        long waitForShutdownStart = -1;

        // Continually process completed log files.
        while (true) {
          if (!reader.isValid()) {
            LOG.info("The checkpoint is out of date. Will reload the checkpoint file.");
            break;
          }

          // Process the new completed log file, if it exists.
          JournalInputStream inputStream = reader.getNextInputStream();
          if (inputStream != null) {
            // reset the shutdown clock, since a new completed log file is being processed.
            waitForShutdownStart = -1;
            LOG.info("Processing a newly completed log file.");
            JournalEntry entry;
            while ((entry = inputStream.getNextEntry()) != null) {
              mMaster.processJournalEntry(entry);
            }
            inputStream.close();
            LOG.info("Finished processing the log file.");
          } else {
            LOG.info("The next complete log file does not exist yet. Sleeping and checking again.");
            if (mInitiateShutdown) {
              if (waitForShutdownStart == -1) {
                waitForShutdownStart = CommonUtils.getCurrentMs();
              } else if ((CommonUtils.getCurrentMs() - waitForShutdownStart) > 1) {
                // There have been no new logs for some time period. It is safe to stop the tailer
                // now.
                // TODO: make the waiting period a configuration parameter.
                LOG.info(mMaster.getProcessorName() + " Journal tailer has been shutdown.");
                return;
              }
            }
          }

          // TODO: make this wait a config parameter
          //CommonUtils.sleepMs(LOG, 5 * Constants.SECOND_MS);
        }

        // TODO: make this wait a config parameter
        //CommonUtils.sleepMs(LOG, 5 * Constants.SECOND_MS);
      } catch (IOException ioe) {
        // Log the error and continue the loop.
        LOG.error(ioe.getMessage());

        if (mInitiateShutdown) {
          LOG.info(mMaster.getProcessorName() + " Journal tailer has been shutdown.");
          return;
        }
        // TODO: make this wait a config parameter
        //CommonUtils.sleepMs(LOG, 5 * Constants.SECOND_MS);
      }
    }
  }
}
