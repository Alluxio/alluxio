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

  private BlockMasterClient mBlockMasterClient;
  private PrintStream mPrintStream;
  private StringBuilder mStringBuilder;
  private int mIndentationLevel = 0;
  private long mSumCapacityBytes = 0;
  private long mSumUsedBytes = 0;
  private Map<String, Long> mSumCapacityBytesOnTierMap = new HashMap<>();
  private Map<String, Long> mSumUsedBytesOnTierMap = new HashMap<>();

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
    if (cl.hasOption("h")) {
      System.out.println(getUsage());
      return 0;
    }

    ReportWorkerOptions options = getOptions(cl);
    generateCapacityReport(options);
    return 0;
  }

  /**
   * Generates capacity report.
   *
   * @param options ReportWorkerOptions to get worker report
   */
  public void generateCapacityReport(ReportWorkerOptions options) throws IOException {
    collectWorkerInfo(options);
    printAggregatedInfo(options);
    mPrintStream.println(mStringBuilder.toString());
  }

  /**
   * Collects worker capacity information.
   *
   * @param options ReportWorkerOptions to get worker report
   */
  private void collectWorkerInfo(ReportWorkerOptions options) throws IOException {
    List<ReportWorkerInfo> workerInfoList = mBlockMasterClient.getWorkerReport(options);
    if (workerInfoList.size() == 0) {
      return;
    }
    mStringBuilder.append(String.format("%n%-17s %-13s %-16s %-13s %-13s %-13s%n",
        "Worker Name", "Type", "Total", "MEM", "SSD", "HDD"));

    for (ReportWorkerInfo workerInfo : workerInfoList) {
      long usedBytes = workerInfo.getUsedBytes();
      long capacityBytes = workerInfo.getCapacityBytes();
      mSumCapacityBytes += capacityBytes;
      mSumUsedBytes += usedBytes;

      String usedPercentageInfo = "";
      if (capacityBytes != 0) {
        int usedPercentage = (int) (100L * usedBytes / capacityBytes);
        usedPercentageInfo = String.format(" (%s%%)", usedPercentage);
      }

      Map<String, Long> totalBytesOnTiers = workerInfo.getCapacityBytesOnTiers();
      for (Map.Entry<String, Long> totalBytesTier : totalBytesOnTiers.entrySet()) {
        String tier = totalBytesTier.getKey();
        long value = totalBytesTier.getValue();
        mSumCapacityBytesOnTierMap.put(tier,
            value + mSumCapacityBytesOnTierMap.getOrDefault(tier, 0L));
      }

      Map<String, Long> usedBytesOnTiers = workerInfo.getUsedBytesOnTiers();
      for (Map.Entry<String, Long> usedBytesTier: usedBytesOnTiers.entrySet()) {
        String tier = usedBytesTier.getKey();
        long value = usedBytesTier.getValue();
        mSumUsedBytesOnTierMap.put(tier,
            value + mSumUsedBytesOnTierMap.getOrDefault(tier, 0L));
      }

      mStringBuilder.append(String.format("%-17s %-13s %-16s %-13s %-13s %-13s%n",
          workerInfo.getAddress().getHost(),
          "Capacity",
          FormatUtils.getSizeFromBytes(capacityBytes),
          FormatUtils.getSizeFromBytes(totalBytesOnTiers.getOrDefault("MEM", 0L)),
          FormatUtils.getSizeFromBytes(totalBytesOnTiers.getOrDefault("SSD", 0L)),
          FormatUtils.getSizeFromBytes(totalBytesOnTiers.getOrDefault("HDD", 0L))));
      mStringBuilder.append(String.format("%-17s %-13s %-16s %-13s %-13s %-13s%n",
          "",
          "Used",
          FormatUtils.getSizeFromBytes(usedBytes) + usedPercentageInfo,
          FormatUtils.getSizeFromBytes(usedBytesOnTiers.getOrDefault("MEM", 0L)),
          FormatUtils.getSizeFromBytes(usedBytesOnTiers.getOrDefault("SSD", 0L)),
          FormatUtils.getSizeFromBytes(usedBytesOnTiers.getOrDefault("HDD", 0L))));
    }
  }

  /**
   * Prints aggregated worker capacity information.
   *
   * @param options ReportWorkerOptions to check if input is invalid
   */
  private void printAggregatedInfo(ReportWorkerOptions options) throws InvalidArgumentException {
    if (options.getWorkerRange().equals(WorkerRange.SPECIFIED)
        && mSumCapacityBytes + mSumUsedBytes == 0) {
      System.out.println(getUsage());
      throw new InvalidArgumentException("Worker IP addresses are invalid.");
    }

    mIndentationLevel = 0;
    print(String.format("Capacity information for %s workers: ",
        options.getWorkerRange().toString().toLowerCase()));

    mIndentationLevel++;
    print("Total Capacity: " + FormatUtils.getSizeFromBytes(mSumCapacityBytes));
    mIndentationLevel++;
    for (Map.Entry<String, Long> totalBytesTier : mSumCapacityBytesOnTierMap.entrySet()) {
      long value = totalBytesTier.getValue();
      print("Tier: " + totalBytesTier.getKey()
          + "  Size: " + FormatUtils.getSizeFromBytes(value));
    }
    mIndentationLevel--;

    print("Used Capacity: "
        + FormatUtils.getSizeFromBytes(mSumUsedBytes));
    mIndentationLevel++;
    for (Map.Entry<String, Long> usedBytesTier : mSumUsedBytesOnTierMap.entrySet()) {
      long value = usedBytesTier.getValue();
      print("Tier: " + usedBytesTier.getKey()
          + "  Size: " + FormatUtils.getSizeFromBytes(value));
    }
    mIndentationLevel--;

    if (mSumCapacityBytes != 0) {
      int usedPercentage = (int) (100L * mSumUsedBytes / mSumCapacityBytes);
      print(String.format("Used Percentage: " + "%s%%", usedPercentage));
      print(String.format("Free Percentage: " + "%s%%", 100 - usedPercentage));
    }
  }

  /**
   * Gets the report worker options.
   *
   * @param cl CommandLine that contains the client options
   * @return ReportWorkerOptions to get report worker information
   */
  private ReportWorkerOptions getOptions(CommandLine cl) throws IOException {
    if (cl.getOptions().length > 1) {
      System.out.println(getUsage());
      throw new InvalidArgumentException("Too many arguments passed in.");
    }
    ReportWorkerOptions reportWorkerOptions = ReportWorkerOptions.defaults();
    reportWorkerOptions.setFieldRange(new HashSet<>(Arrays.asList(ReportWorkerInfoField.ADDRESS,
        ReportWorkerInfoField.CAPACITY_BYTES, ReportWorkerInfoField.CAPACITY_BYTES_ON_TIERS,
        ReportWorkerInfoField.USED_BYTES, ReportWorkerInfoField.USED_BYTES_ON_TIERS)));
    if (cl.hasOption("live")) {
      reportWorkerOptions.setWorkerRange(WorkerRange.LIVE);
    } else if (cl.hasOption("lost")) {
      reportWorkerOptions.setWorkerRange(WorkerRange.LOST);
    } else if (cl.hasOption("worker")) {
      reportWorkerOptions.setWorkerRange(WorkerRange.SPECIFIED);
      String addressString = cl.getOptionValue("worker");
      String[] addressArray = addressString.split(",");
      // Addresses in ReportWorkerOptions is only used when WorkerRange is SPECIFIED
      reportWorkerOptions.setAddresses(new HashSet<>(Arrays.asList(addressArray)));
    }
    return reportWorkerOptions;
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
   * @return capacity command usage
   */
  public static String getUsage() {
    return "alluxio fsadmin report capacity [filter arg]\n"
        + "Report Alluxio capacity information.\n"
        + "Where [filter arg] is an optional argument. If no arguments passed in, "
        + "capacity information of all workers will be printed out.\n"
        + "[filter arg] can be one of the following:\n"
        + "    -live                         Live workers\n"
        + "    -lost                         Lost workers\n"
        + "    -worker <worker_ip_addresses> Specified workers, IP addresses separated by \",\"\n";
  }
}
