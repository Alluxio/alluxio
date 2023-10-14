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

package alluxio.cli.fsadmin.report;

import alluxio.client.block.BlockMasterClient;
import alluxio.client.meta.MetaMasterClient;
import alluxio.grpc.MasterInfo;
import alluxio.grpc.MasterInfoField;
import alluxio.wire.BlockMasterInfo;
import alluxio.wire.BlockMasterInfo.BlockMasterInfoField;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Prints Alluxio cluster summarized information.
 */
public class SummaryCommand {
  private static final Logger LOG = LoggerFactory.getLogger(SummaryCommand.class);
  private final MetaMasterClient mMetaMasterClient;
  private final BlockMasterClient mBlockMasterClient;
  private final PrintStream mPrintStream;
  private final String mDateFormatPattern;

  /**
   * Creates a new instance of {@link SummaryCommand}.
   *
   * @param metaMasterClient client to connect to meta master
   * @param blockMasterClient client to connect to block master
   * @param dateFormatPattern the pattern to follow when printing the date
   * @param printStream stream to print summary information to
   */
  public SummaryCommand(MetaMasterClient metaMasterClient,
      BlockMasterClient blockMasterClient, String dateFormatPattern, PrintStream printStream) {
    mMetaMasterClient = metaMasterClient;
    mBlockMasterClient = blockMasterClient;
    mPrintStream = printStream;
    mDateFormatPattern = dateFormatPattern;
  }

  /**
   * Runs report summary command.
   *
   * @return 0 on success, 1 otherwise
   */
  public int run() throws IOException {
    Set<MasterInfoField> masterInfoFilter = new HashSet<>(Arrays
        .asList(MasterInfoField.LEADER_MASTER_ADDRESS, MasterInfoField.WEB_PORT,
            MasterInfoField.RPC_PORT, MasterInfoField.START_TIME_MS,
            MasterInfoField.UP_TIME_MS, MasterInfoField.VERSION,
            MasterInfoField.SAFE_MODE, MasterInfoField.ZOOKEEPER_ADDRESSES,
            MasterInfoField.RAFT_JOURNAL, MasterInfoField.RAFT_ADDRESSES,
            MasterInfoField.MASTER_VERSION));
    MasterInfo masterInfo = mMetaMasterClient.getMasterInfo(masterInfoFilter);

    Set<BlockMasterInfoField> blockMasterInfoFilter = new HashSet<>(Arrays
        .asList(BlockMasterInfoField.LIVE_WORKER_NUM, BlockMasterInfoField.LOST_WORKER_NUM,
            BlockMasterInfoField.CAPACITY_BYTES, BlockMasterInfoField.USED_BYTES,
            BlockMasterInfoField.FREE_BYTES, BlockMasterInfoField.CAPACITY_BYTES_ON_TIERS,
            BlockMasterInfoField.USED_BYTES_ON_TIERS));
    BlockMasterInfo blockMasterInfo = mBlockMasterClient.getBlockMasterInfo(blockMasterInfoFilter);

    ObjectMapper objectMapper = new ObjectMapper();
    SummaryOutput summaryInfo = new SummaryOutput(masterInfo, blockMasterInfo);
    try {
      String json = objectMapper.writeValueAsString(summaryInfo);
      mPrintStream.println(json);
    } catch (JsonProcessingException e) {
      mPrintStream.println("Failed to convert summaryInfo output to JSON. "
          + "Check the command line log for the detailed error message.");
      LOG.error("Failed to output JSON object {}", summaryInfo);
      e.printStackTrace();
      return -1;
    }

    return 0;
  }
}
