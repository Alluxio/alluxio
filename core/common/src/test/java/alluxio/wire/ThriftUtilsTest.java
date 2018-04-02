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

package alluxio.wire;

import static org.junit.Assert.assertEquals;

import alluxio.thrift.TTtlAction;

import org.junit.Test;

import java.util.Collections;

/**
 * Unit tests for {@link ThriftUtils}.
 */
public class ThriftUtilsTest {
  @Test
  public void toThrift() {
    assertEquals(100L, ThriftUtils.toThrift(getBlockInfo()).getBlockId());
    assertEquals(100L, ThriftUtils.toThrift(getBlockLocation()).getWorkerId());
    assertEquals(5, ThriftUtils.toThrift(getBlockMasterInfo()).getLiveWorkerNum());
    assertEquals("time", ThriftUtils.toThrift(getCommandLineJobInfo()).getCommand());
    assertEquals(100L, ThriftUtils.toThrift(getFileBlockInfo()).getOffset());
    assertEquals("rambo", ThriftUtils.toThrift(getFileInfo()).getName());
    assertEquals("info.log", ThriftUtils.toThrift(getJobConfInfo()).getOutputFile());
    assertEquals(100L, ThriftUtils.toThrift(getLineageInfo()).getId());
    assertEquals(19998, ThriftUtils.toThrift(getMasterInfo()).getRpcPort());
    assertEquals(TTtlAction.Free.name().toLowerCase(), ThriftUtils.toThrift(TtlAction.FREE).name()
        .toLowerCase());
    assertEquals(100L, ThriftUtils.toThrift(getWorkerInfo()).getUsedBytes());
    assertEquals("host1", ThriftUtils.toThrift(getWorkerNetAddress()).getHost());
  }

  @Test
  public void fromThrift() {
    assertEquals(100L, ThriftUtils.fromThrift(getBlockInfoThrift()).getBlockId());
    assertEquals(100L, ThriftUtils.fromThrift(getBlockLocationThrift()).getWorkerId());
    assertEquals(5, ThriftUtils.fromThrift(getBlockMasterInfoThrift()).getLiveWorkerNum());
    assertEquals("time", ThriftUtils.fromThrift(getCommandLineJobInfoThrift()).getCommand());
    assertEquals(100L, ThriftUtils.fromThrift(getFileBlockInfoThrift()).getOffset());
    assertEquals("rambo", ThriftUtils.fromThrift(getFileInfoThrift()).getName());
    assertEquals("info.log", ThriftUtils.fromThrift(getJobConfInfoThrift()).getOutputFile());
    assertEquals(100L, ThriftUtils.fromThrift(getLineageInfoThrift()).getId());
    assertEquals(19998, ThriftUtils.fromThrift(getMasterInfoThrift()).getRpcPort());
    assertEquals(TtlAction.FREE.name().toLowerCase(), ThriftUtils.fromThrift(TTtlAction.Free)
        .name().toLowerCase());
    assertEquals(100L, ThriftUtils.fromThrift(getWorkerInfoThrift()).getUsedBytes());
    assertEquals("host1", ThriftUtils.fromThrift(getWorkerNetAddressThrift()).getHost());
  }

  private BlockInfo getBlockInfo() {
    BlockInfo i = new BlockInfo();
    i.setBlockId(100L);
    return i;
  }

  private alluxio.thrift.BlockInfo getBlockInfoThrift() {
    return new alluxio.thrift.BlockInfo(100, 100L,
        Collections.<alluxio.thrift.BlockLocation>emptyList());
  }

  private BlockLocation getBlockLocation() {
    BlockLocation l = new BlockLocation();
    l.setWorkerId(100L);
    return l;
  }

  private alluxio.thrift.BlockLocation getBlockLocationThrift() {
    return new alluxio.thrift.BlockLocation(100L, new alluxio.thrift.WorkerNetAddress("host", 0, 0,
        0, "", null), "tierAlias");
  }

  private BlockMasterInfo getBlockMasterInfo() {
    BlockMasterInfo b = new BlockMasterInfo();
    b.setLiveWorkerNum(5);
    return b;
  }

  private alluxio.thrift.BlockMasterInfo getBlockMasterInfoThrift() {
    return new alluxio.thrift.BlockMasterInfo().setLiveWorkerNum(5);
  }

  private CommandLineJobInfo getCommandLineJobInfo() {
    CommandLineJobInfo i = new CommandLineJobInfo();
    i.setCommand("time");
    return i;
  }

  private alluxio.thrift.CommandLineJobInfo getCommandLineJobInfoThrift() {
    return new alluxio.thrift.CommandLineJobInfo("time", new alluxio.thrift.JobConfInfo(
        "outputFile"));
  }

  private FileBlockInfo getFileBlockInfo() {
    FileBlockInfo i = new FileBlockInfo();
    i.setOffset(100L);
    return i;
  }

  private alluxio.thrift.FileBlockInfo getFileBlockInfoThrift() {
    return new alluxio.thrift.FileBlockInfo(new alluxio.thrift.BlockInfo(100L, 200L,
        Collections.<alluxio.thrift.BlockLocation>emptyList()), 100L,
        Collections.<alluxio.thrift.WorkerNetAddress>emptyList(), Collections.<String>emptyList());
  }

  private FileInfo getFileInfo() {
    FileInfo i = new FileInfo();
    i.setName("rambo");
    return i;
  }

  private alluxio.thrift.FileInfo getFileInfoThrift() {
    return new alluxio.thrift.FileInfo().setName("rambo")
        .setBlockIds(Collections.<Long>emptyList())
        .setFileBlockInfos(Collections.<alluxio.thrift.FileBlockInfo>emptyList());
  }

  private JobConfInfo getJobConfInfo() {
    JobConfInfo j = new JobConfInfo();
    j.setOutputFile("info.log");
    return j;
  }

  private alluxio.thrift.JobConfInfo getJobConfInfoThrift() {
    alluxio.thrift.JobConfInfo j = new alluxio.thrift.JobConfInfo();
    j.setOutputFile("info.log");
    return j;
  }

  private LineageInfo getLineageInfo() {
    LineageInfo i = new LineageInfo();
    i.setId(100L);
    return i;
  }

  private alluxio.thrift.LineageInfo getLineageInfoThrift() {
    return new alluxio.thrift.LineageInfo()
        .setId(100L)
        .setChildren(Collections.<Long>emptyList())
        .setInputFiles(Collections.<String>emptyList())
        .setParents(Collections.<Long>emptyList())
        .setOutputFiles(Collections.<String>emptyList())
        .setJob(
            new alluxio.thrift.CommandLineJobInfo("command", new alluxio.thrift.JobConfInfo(
                "outputFile")));
  }

  private MasterInfo getMasterInfo() {
    MasterInfo m = new MasterInfo();
    m.setRpcPort(19998);
    return m;
  }

  private alluxio.thrift.MasterInfo getMasterInfoThrift() {
    return new alluxio.thrift.MasterInfo().setRpcPort(19998);
  }

  private WorkerInfo getWorkerInfo() {
    WorkerInfo w = new WorkerInfo();
    w.setUsedBytes(100L);
    w.setAddress(getWorkerNetAddress());
    return w;
  }

  private alluxio.thrift.WorkerInfo getWorkerInfoThrift() {
    return new alluxio.thrift.WorkerInfo().setUsedBytes(100L).setAddress(
        getWorkerNetAddressThrift());
  }

  private WorkerNetAddress getWorkerNetAddress() {
    return new WorkerNetAddress().setHost("host1").setDataPort(1).setRpcPort(2).setWebPort(3);
  }

  private alluxio.thrift.WorkerNetAddress getWorkerNetAddressThrift() {
    return new alluxio.thrift.WorkerNetAddress().setHost("host1").setDataPort(1).setRpcPort(2)
        .setWebPort(3);
  }
}
