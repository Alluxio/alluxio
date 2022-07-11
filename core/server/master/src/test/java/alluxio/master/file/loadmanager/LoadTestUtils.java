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

package alluxio.master.file.loadmanager;

import alluxio.grpc.Block;
import alluxio.grpc.BlockStatus;
import alluxio.util.CommonUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.grpc.Status;

import java.util.List;
import java.util.Random;
import java.util.stream.LongStream;

public final class LoadTestUtils {
  private LoadTestUtils() {}

  public static List<BlockStatus> generateRandomBlockStatus(
      List<Block> blocks, double failureRate) {
    ImmutableList.Builder<BlockStatus> blockStatus = ImmutableList.builder();
    for (Block block : blocks) {
      if (Math.random() > failureRate) {
        blockStatus.add(BlockStatus.newBuilder()
            .setBlock(block)
            .setCode(Status.OK.getCode().value())
            .build());
      }
      else {
        blockStatus.add(BlockStatus.newBuilder()
            .setBlock(block)
            .setCode((int) (Math.random() * 10) + 1)
            .setRetryable(Math.random() > 0.5)
            .build());
      }
    }
    return blockStatus.build();
  }

  public static  List<FileInfo> generateRandomFileInfo(
      int fileCount, int blockCountPerFile, long blockSizeLimit) {
    List<FileInfo> fileInfos = Lists.newArrayList();
    for (int i = 0; i < fileCount; i++) {
      FileInfo info = createFileInfo(blockCountPerFile, blockSizeLimit);
      fileInfos.add(info);
    }
    return fileInfos;
  }

  private static FileInfo createFileInfo(int blockCount, long blockSizeLimit) {
    Random random = new Random();
    FileInfo info = new FileInfo();
    String ufs = CommonUtils.randomAlphaNumString(6);
    long blockSize = Math.abs(random.nextLong() % blockSizeLimit);
    info.setUfsPath(ufs);
    info.setBlockSizeBytes(blockSize);
    List<Long> blockIds = LongStream.range(0, blockCount)
        .map(i -> random.nextLong())
        .boxed()
        .collect(ImmutableList.toImmutableList());
    info.setBlockIds(blockIds);
    info.setFileBlockInfos(blockIds.stream()
        .map(LoadTestUtils::createFileBlockInfo)
        .collect(ImmutableList.toImmutableList()));
    return info;
  }

  private static FileBlockInfo createFileBlockInfo(long id) {
    FileBlockInfo fileBlockInfo = new FileBlockInfo();
    BlockInfo blockInfo = new BlockInfo();
    blockInfo.setBlockId(id);
    fileBlockInfo.setBlockInfo(blockInfo);
    fileBlockInfo.setOffset(new Random().nextInt(1000));
    return fileBlockInfo;
  }
}
