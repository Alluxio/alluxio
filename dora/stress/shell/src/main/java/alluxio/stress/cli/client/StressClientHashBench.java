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

package alluxio.stress.cli.client;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.dora.WorkerLocationPolicy;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.membership.WorkerClusterView;
import alluxio.stress.cli.Benchmark;
import alluxio.stress.client.HashParameters;
import alluxio.stress.client.HashTaskResult;
import alluxio.util.ExceptionUtils;
import alluxio.wire.WorkerIdentity;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.beust.jcommander.ParametersDelegate;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * A benchmark tool measuring various hashing policy.
 * */
public class StressClientHashBench extends Benchmark<HashTaskResult> {

  private static final Logger LOG = LoggerFactory.getLogger(StressClientHashBench.class);

  @ParametersDelegate
  private final HashParameters mParameters = new HashParameters();

  private List<String> mFileNamesList;

  private WorkerClusterView mWorkers;
  private List<WorkerInfo> mWorkerInfos;

  @Override
  public String getBenchDescription() {
    return String.join("\n", ImmutableList.of(
        "A benchmarking tool for various hashing policy",
        "This test will measure:\n"
            + "1. Time Cost: The time consumed after the file "
            + "is allocated once to judge the efficiency of the algorithm.\n\n"
            + "2. Standard Deviation: The standard deviation of the number assigned to each "
            + "worker to judge the uniformity of the algorithm.\n\n"
            + "3. File Reallocated: After randomly deleting a Worker, redistribute the File again, "
            + "and count how many files assigned to the Worker have changed. "
            + "The fewer the number of File moves, "
            + "the better the consistency of the algorithm.\n\n",
        "",

        "Example:",
        "# This invokes the hashing tests",
        "# 5 hash algorithms will be tested: CONSISTENT, JUMP, KETAMA, MAGLEV, MULTI_PROBE",
        "# 10 workers will be used",
        "# 10000 virtual nodes will be used",
        "# 10 workers will be used",
        "# 1000 worker replicas will be used",
        "# the size of lookup table is 65537 (must be a prime)",
        "# the num of probes is 21",
        "# The report will be generated under the current path",
        "# The number of simulation test files is 1,000,000",
        "$ bin/alluxio exec class alluxio.stress.cli.client.StressClientHashBench -- \\"
        + "--hash-policy CONSISTENT,JUMP,KETAMA,MAGLEV,MULTI_PROBE \\"
        + "--virtual-node-num 10000 --worker-num 10 --node-replicas 1000 \\"
        + "--lookup-size 65537 --probe-num 21 --report-path . \\"
        + "--file-num 1000000",
        ""
    ));
  }

  @Override
  public HashTaskResult runLocal() throws Exception {
    HashTaskResult result = null;

    // Add timestamp to test report file name
    String reportFileName = "Stress-Client-Hash-Bench-"
        + new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date()) + ".txt";

    // If the custom path entered by the user ends with "/", remove this "/".
    if (mParameters.mReportPath.endsWith("/")) {
      mParameters.mReportPath =
          mParameters.mReportPath.substring(0, mParameters.mReportPath.length() - 1);
    }

    // If the folder does not exist, create the folder.
    File filePath = new File(mParameters.mReportPath);
    if  (!filePath.exists()  && !filePath.isDirectory()) {
      filePath.mkdirs();
    }
    FileOutputStream fos = new FileOutputStream(mParameters.mReportPath + "/" + reportFileName);

    try {
      result = runHashBench();
      LOG.debug("Hash benchmark finished with result: {}", result);

      // Write an explanation of the hash test results.
      String resultDescription =
          "Each hash algorithm report consists of three results:\n\n"
          + "1. Time Cost: The time consumed after the file "
          + "is allocated once to judge the efficiency of the algorithm.\n\n"
          + "2. Standard Deviation: The standard deviation of the number assigned to each Worker "
          + "to judge the uniformity of the algorithm.\n\n"
          + "3. File Reallocated: After randomly deleting a Worker, redistribute the File again, "
          + "and count how many files assigned to the Worker have changed. "
          + "The fewer the number of File moves, the better the consistency of the algorithm.\n\n";

      fos.write(resultDescription.getBytes());

      // Write the report into outputFile
      fos.write(result.toString().getBytes());

      // Close the streams
      fos.flush();
      fos.close();
      fos = null;
      return result;
    } catch (Exception e) {
      if (result == null) {
        LOG.error("Failed run Hash Benchmark", e);
        result = new HashTaskResult();
        result.setParameters(mParameters);
        result.addError(ExceptionUtils.asPlainText(e));
      }
      return result;
    } finally {
      if (fos != null) {
        fos.close();
      }
    }
  }

  @Override
  public void prepare() throws Exception {
    mFileNamesList = new ArrayList<>();

    // Prepare the file name of the simulation file. The file name is randomly generated.
    for (int i = 0; i < mParameters.mFileNum; i++) {
      mFileNamesList.add(randomString(10));
    }

    // Generate simulated Worker.
    mWorkerInfos = new ArrayList<>();
    for (int i = 0; i < mParameters.mWorkerNum; i++) {
      mWorkerInfos.add(
          new WorkerInfo()
              .setIdentity(ofLegacyId(i))
              .setAddress(new WorkerNetAddress()
                  .setHost("worker" + i).setRpcPort(29998).setDataPort(29999).setWebPort(30000))
              .setCapacityBytes(1024)
              .setUsedBytes(0)
      );
    }
    mWorkers = new WorkerClusterView(mWorkerInfos);
  }

  /**
   * generate random string as file name.
   * @param len
   * @return random string
   */
  private static String randomString(int len) {
    // Randomly generate a string of length len
    String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-.";
    Random random = new Random();
    StringBuffer stringBuffer = new StringBuffer();
    for (int i = 0; i < len; i++) {
      int number = random.nextInt(64);
      stringBuffer.append(str.charAt(number));
    }
    return stringBuffer.toString();
  }

  /**
   * @param id id
   * @return the identity that is based on the specified numeric id
   */
  public static WorkerIdentity ofLegacyId(long id) {
    return WorkerIdentity.ParserV0.INSTANCE.fromLong(id);
  }

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    mainInternal(args, new StressClientHashBench());
  }

  // Benchmark for running hashing algorithms
  private HashTaskResult runHashBench() throws Exception {
    HashTaskResult result = new HashTaskResult();
    // Extract the hash algorithm passed in by the user.
    // Different hash algorithms are separated by commas.
    List<String> hashPolicyList = Arrays.asList((mParameters.mHashPolicy.split(",")));
    for (String policy: hashPolicyList) {
      // Determine whether the hash algorithm name passed by the user is legal
      if (policy.equals("CONSISTENT") || policy.equals("JUMP") || policy.equals("KETAMA")
          || policy.equals("MAGLEV") || policy.equals("MULTI_PROBE")) {
        result.addSingleTestResult(testHashPolicy(policy));
      }
      else {
        System.out.println(policy + " is not supported!");
      }
    }
    return result;
  }

  // Test a hash algorithm
  private HashTaskResult.SingleTestResult testHashPolicy(String hashPolicy)
      throws ResourceExhaustedException {
    long startTime = System.currentTimeMillis();

    // Add the parameters set by the user to the conf file.
    InstancedConfiguration conf = new InstancedConfiguration(Configuration.copyProperties());
    conf.set(PropertyKey.USER_WORKER_SELECTION_POLICY, hashPolicy);
    if (hashPolicy.equals("CONSISTENT")) {
      conf.set(PropertyKey.USER_CONSISTENT_HASH_VIRTUAL_NODE_COUNT_PER_WORKER,
          mParameters.mVirtualNodeNum);
    }
    else if (hashPolicy.equals("KETAMA")) {
      conf.set(PropertyKey.USER_KETAMA_HASH_REPLICAS, mParameters.mNodeReplicas);
    }
    else if (hashPolicy.equals("MAGLEV")) {
      conf.set(PropertyKey.USER_MAGLEV_HASH_LOOKUP_SIZE, mParameters.mLookupSize);
    }
    else if (hashPolicy.equals("MULTI_PROBE")) {
      conf.set(PropertyKey.USER_MULTI_PROBE_HASH_PROBE_NUM, mParameters.mProbeNum);
    }
    WorkerLocationPolicy policy = WorkerLocationPolicy.Factory.create(conf);

    // Record the number of files allocated to each worker
    HashMap<WorkerIdentity, Integer> workerCount = new HashMap<>();

    // 保存每个文件存储到哪个worker
    List<WorkerIdentity> fileWorkerList = new ArrayList<>();

    // Simulate the process of hashing a file and then assigning it to a worker
    for (int i = 0; i < mParameters.mFileNum; i++) {
      String fileName = mFileNamesList.get(i);
      List<BlockWorkerInfo> workers = policy.getPreferredWorkers(mWorkers, fileName, 1);
      for (BlockWorkerInfo worker : workers) {
        if (workerCount.containsKey(worker.getIdentity())) {
          workerCount.put(worker.getIdentity(), workerCount.get(worker.getIdentity()) + 1);
        } else {
          workerCount.put(worker.getIdentity(), 1);
        }
        fileWorkerList.add(worker.getIdentity());
      }
    }
    long endTime = System.currentTimeMillis();
    long timeCost = endTime - startTime;

    // Count how many files are allocated on each worker.
    List<Integer> workerCountList = new ArrayList<>(workerCount.values());

    // Calculate the standard deviation of the number of files allocated to each worker above.
    double standardDeviation = getStandardDeviation(workerCountList);

    // randomly removing a Worker
    mWorkerInfos.remove((int) (Math.random() * mParameters.mWorkerNum));
    mWorkers = new WorkerClusterView(mWorkerInfos);

    // Randomly remove a Worker and re-simulate the file allocation process
    // to determine the consistency of the algorithm.
    policy = WorkerLocationPolicy.Factory.create(conf);
    workerCount.clear();
    List<WorkerIdentity> newFileWorkerList = new ArrayList<>();

    for (int i = 0; i < mParameters.mFileNum; i++) {
      String fileName = mFileNamesList.get(i);
      List<BlockWorkerInfo> workers = policy.getPreferredWorkers(mWorkers, fileName, 1);
      for (BlockWorkerInfo worker : workers) {
        newFileWorkerList.add(worker.getIdentity());
      }
    }

    // Compare how many files stored in the Worker have changed
    int fileReallocatedNum = 0;
    for (int i = 0; i < mParameters.mFileNum; i++) {
      if (fileWorkerList.get(i) != newFileWorkerList.get(i)) {
        ++fileReallocatedNum;
      }
    }

    return new HashTaskResult.SingleTestResult(
        hashPolicy, timeCost, standardDeviation, fileReallocatedNum);
  }

  /**
   * @param list Stores the number used to calculate the standard deviation
   * @return Standard deviation of all numbers in List
   */
  private double getStandardDeviation(List<Integer> list) {
    double variance = 0;
    double average = 0;
    for (int i = 0; i < list.size(); i++) {
      average += list.get(i);
    }
    average /= list.size();
    for (int i = 0; i < list.size(); i++) {
      variance += Math.pow(list.get(i) - average, 2);
    }
    variance /= list.size();

    // Keep to two decimal places
    return Math.round(Math.sqrt(variance) * 100) / 100.0;
  }
}
