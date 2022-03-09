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

//package alluxio.cli.fs.command.cmd;
//
//import alluxio.client.job.JobMasterClient;
//import alluxio.job.CmdConfig;
//import alluxio.retry.RetryPolicy;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//
///**
// * Abstract class for handling submission for a Cmd.
// */
//public class CmdAttempt {
//  private static final Logger LOG = LoggerFactory.getLogger(CmdAttempt.class);
//
//  private CmdAttempt() {
//  }
//
//  /**
//   * Submit a Cmd job.
//   * @param client
//   * @param retryPolicy
//   * @param cmdConfig
//   * @return a jobControlId
//   */
//  public static boolean submit(JobMasterClient client, RetryPolicy retryPolicy,
//          CmdConfig cmdConfig) {
//    Long mJobControlId;
//    while (retryPolicy.attempt()) {
//      mJobControlId = null;
//      try {
//        mJobControlId = client.submit(cmdConfig);
//        System.out.println(String.format("Submitted a CMD job and" + "received jobControlId = %s",
//                mJobControlId));
//      } catch (IOException e) {
//        int retryCount = retryPolicy.getAttemptCount();
//        System.out.println(String.format("Retry %d Failed to start cmd job with error: %s",
//                retryCount, e.getMessage()));
//        LOG.warn("Retry {} Failed to get status for job control id (jobControlId={}) {}",
//                retryCount, mJobControlId, e);
//        continue;
//        // Do nothing. This will be counted as a failed attempt
//      }
//      return true;
//    }
//    //logFailed();
//    return false;
//  }
//
////  protected abstract void logFailedAttempt(CmdInfo cmdInfo);
////
////  protected abstract void logFailed();
////
////  protected abstract void logCompleted();
//}
