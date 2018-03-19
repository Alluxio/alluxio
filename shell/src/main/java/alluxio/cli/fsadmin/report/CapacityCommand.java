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
import alluxio.client.block.options.ReportWorkerOptions;
import alluxio.client.block.options.ReportWorkerOptions.ReportWorkerInfoField;
import alluxio.client.block.options.ReportWorkerOptions.WorkerRange;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.wire.ReportWorkerInfo;

import com.google.common.base.Strings;
import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Prints Alluxio capacity information.
 */
public class CapacityCommand {
  private static final int INDENT_SIZE = 4;
  private static final String USAGE = "alluxio fsadmin report capacity [filter arg]\n"
      + "Report Alluxio capacity information.\n"
      + "Where filter arg is an optional argument, if no arguments passed in, "
      + "cluster capacity information will be printed out."
      + "filter_arg can be one of the following:\n"
      + "    -live                         Live workers\n"
      + "    -lost                         Lost workers\n"
      + "    -worker <worker_ip_addresses> Specified workers, IP addresses separated by \",\"";

  private BlockMasterClient mBlockMasterClient;
  private PrintStream mPrintStream;
  private int mIndentationLevel = 0;
  private StringBuilder mStringBuilder;

   /**
   * Creates a new instance of {@link CapacityCommand}.
   *
   * @param blockMasterClient client to connect to block master
   * @param printStream stream to print summary information to
   */
  public CapacityCommand(BlockMasterClient blockMasterClient, PrintStream printStream) {
    mBlockMasterClient = blockMasterClient;
    mPrintStream = printStream;
    mStringBuilder = new StringBuilder();
  }

  /**
   * Runs report capacity command.
   *
   * @param cl CommandLine to get client options
   * @return 0 on success, 1 otherwise
   */
  public int run(CommandLine cl) throws IOException {
    ReportWorkerOptions options = getOptions(cl);
    printWorkerCapacityInfo(options);
    return 0;
  }

  /**
   * Prints Alluxio capacity information.
   *
   * @param options ReportWorkerOptions to define the report worker info range
   */
  public void printWorkerCapacityInfo(ReportWorkerOptions options) throws IOException {
    mIndentationLevel = 0;
    print(String.format("Capacity Information for %s Workers: ", options.getWorkerRange()));
    List<ReportWorkerInfo> list = mBlockMasterClient.getReportWorkerInfoList(options);

    // Collect the worker information for integration
    long sumCapacityBytes = 0;
    long sumUsedBytes = 0;
    Map<String, Long> sumTotalBytesOnTiersMap = initializeTierMap();
    Map<String, Long> sumUsedBytesOnTiersMap = initializeTierMap();

    // Cache the worker capacity information
    for (ReportWorkerInfo workerInfo : list) {
      mIndentationLevel = 0;
      String sepLineInfo = String.format("%n----------- Node %s -----------",
          workerInfo.getAddress().getHost());
      cache(sepLineInfo);
      mIndentationLevel++;
      cache("Start Time: "
          + CommonUtils.convertMsToDate(workerInfo.getStartTimeMs()));
      cache("Last Contact Second: " + workerInfo.getLastContactSec());

      long capacityBytes = workerInfo.getCapacityBytes();
      long usedBytes = workerInfo.getUsedBytes();
      sumCapacityBytes += capacityBytes;
      sumUsedBytes += usedBytes;

      cache("Total Capacity: "
          + FormatUtils.getSizeFromBytes(capacityBytes));

      Map<String, Long> totalBytesOnTiers = workerInfo.getCapacityBytesOnTiers();
      mIndentationLevel++;
      if (totalBytesOnTiers != null) {
        for (Map.Entry<String, Long> totalBytesTier : totalBytesOnTiers.entrySet()) {
          String tier = totalBytesTier.getKey();
          long value = totalBytesTier.getValue();
          sumTotalBytesOnTiersMap.put(tier, value + sumTotalBytesOnTiersMap.get(tier));
          cache("Tier: " + tier + "  Size: "
              + FormatUtils.getSizeFromBytes(value));
        }
      }

      mIndentationLevel--;
      cache("Used Capacity: "
          + FormatUtils.getSizeFromBytes(usedBytes));

      Map<String, Long> usedBytesOnTiers = workerInfo.getUsedBytesOnTiers();
      mIndentationLevel++;
      if (usedBytesOnTiers != null) {
        for (Map.Entry<String, Long> usedBytesTier: usedBytesOnTiers.entrySet()) {
          String tier = usedBytesTier.getKey();
          long value = usedBytesTier.getValue();
          sumUsedBytesOnTiersMap.put(tier, value + sumUsedBytesOnTiersMap.get(tier));
          cache("Tier: " + tier + "  Size: "
              + FormatUtils.getSizeFromBytes(value));
        }
      }

      mIndentationLevel--;
      if (capacityBytes != 0) {
        int usedPercentage = (int) (100L * usedBytes / capacityBytes);
        cache(String.format("Used Percentage: " + "%s%%", usedPercentage));
        cache(String.format("Free Percentage: " + "%s%%", 100 - usedPercentage));
      }
    }

    // Prints the information of Alluxio worker integration
    if (options.getWorkerRange().equals(WorkerRange.SPECIFIED)
        && sumCapacityBytes + sumUsedBytes == 0) {
      System.out.println(USAGE);
      throw new InvalidArgumentException("Worker IP addresses are invalid.");
    }

    mIndentationLevel = 1;
    print("Total Capacity: " + FormatUtils.getSizeFromBytes(sumCapacityBytes));
    mIndentationLevel++;
    for (Map.Entry<String, Long> totalBytesTier : sumTotalBytesOnTiersMap.entrySet()) {
      long value = totalBytesTier.getValue();
      if (value != 0) {
        print("Tier: " + totalBytesTier.getKey()
            + "  Size: " + FormatUtils.getSizeFromBytes(value));
      }
    }

    mIndentationLevel--;
    print("Used Capacity: "
        + FormatUtils.getSizeFromBytes(sumUsedBytes));
    mIndentationLevel++;
    for (Map.Entry<String, Long> usedBytesTier : sumUsedBytesOnTiersMap.entrySet()) {
      long value = usedBytesTier.getValue();
      if (value != 0) {
        print("Tier: " + usedBytesTier.getKey()
            + "  Size: " + FormatUtils.getSizeFromBytes(value));
      }
    }

    mIndentationLevel--;
    if (sumCapacityBytes != 0) {
      int usedPercentage = (int) (100L * sumUsedBytes / sumCapacityBytes);
      print(String.format("Used Percentage: " + "%s%%", usedPercentage));
      print(String.format("Free Percentage: " + "%s%%", 100 - usedPercentage));
    }

    // Print cached worker information
    mPrintStream.println(mStringBuilder.toString());
  }

  /**
   * Gets the report worker options.
   *
   * @param cl CommandLine that contains the client options
   * @return ReportWorkerOptions to get report worker information
   */
  private ReportWorkerOptions getOptions(CommandLine cl) throws IOException {
    ReportWorkerOptions options = ReportWorkerOptions.defaults();
    options.setFieldRange(new HashSet<>(Arrays.asList(ReportWorkerInfoField.ADDRESS,
        ReportWorkerInfoField.CAPACITY_BYTES, ReportWorkerInfoField.CAPACITY_BYTES_ON_TIERS,
        ReportWorkerInfoField.ID, ReportWorkerInfoField.LAST_CONTACT_SEC,
        ReportWorkerInfoField.START_TIME_MS, ReportWorkerInfoField.USED_BYTES,
        ReportWorkerInfoField.USED_BYTES_ON_TIERS)));
    if (cl.hasOption("live")) {
      options.setWorkerRange(WorkerRange.LIVE);
    } else if (cl.hasOption("lost")) {
      options.setWorkerRange(WorkerRange.LOST);
    } else if (cl.hasOption("worker")) {
      options.setWorkerRange(WorkerRange.SPECIFIED);
      String addressString = cl.getOptionValue("worker");
      String[] addressArray = addressString.split(",");
      options.setAddresses(new HashSet<>(Arrays.asList(addressArray)));
    } else if (cl.getOptions().length != 0) {
      System.out.println(USAGE);
      throw new InvalidArgumentException("report capacity filter tag is not valid.");
    }
    return options;
  }

  /**
   * @return initialized tier map
   */
  private Map<String, Long> initializeTierMap() {
    Map<String, Long> map = new HashMap<>();
    map.put("MEM", 0L);
    map.put("SSD", 0L);
    map.put("HDD", 0L);
    return map;
  }

  /**
   * Prints indented information.
   *
   * @param text information to print
   */
  private void print(String text) {
    String indent = Strings.repeat(" ", mIndentationLevel * INDENT_SIZE);
    mPrintStream.println(indent + text);
  }

  /**
   * Caches indented information.
   *
   * @param text information to cache
   */
  private void cache(String text) {
    String indentedString = String.format("%s%s\n",
        Strings.repeat(" ", mIndentationLevel * INDENT_SIZE), text);
    mStringBuilder.append(indentedString);
  }
}
