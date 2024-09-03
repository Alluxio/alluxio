package alluxio.cli.fs.command;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.grpc.BlockChecksum;
import alluxio.grpc.BlockLocation;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.GetBlockChecksumRequest;
import alluxio.grpc.GetBlockChecksumResponse;
import alluxio.grpc.GetStatusPOptions;
import alluxio.master.block.BlockId;
import alluxio.resource.CloseableResource;
import alluxio.util.CRC64;
import alluxio.wire.BlockLocationInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import javax.annotation.Nullable;
import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.zip.Checksum;

public class CRC64CheckCommand extends AbstractFileSystemCommand{
  public CRC64CheckCommand(
      @Nullable FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  protected void runPlainPath(AlluxioURI plainPath, CommandLine cl)
      throws AlluxioException, IOException {
    /*
    CRC64 aaa = new CRC64();
    aaa.update("aaa".getBytes(), 3);
    System.out.println(Long.toHexString(aaa.getValue()));
    if (true) {
      return;
    }

     */

    URIStatus ufsStatus = mFileSystem.getStatus(
        plainPath, GetStatusPOptions.newBuilder().setDirectUfsAccess(true).build());
    if (ufsStatus.getXAttr() == null || !ufsStatus.getXAttr().containsKey(Constants.CRC64_KEY)) {
      System.out.println("UFS does not store crc64");
//      return;
    }

    long crc64Value = 0;
    URIStatus status = mFileSystem.getStatus(plainPath);
    if (status.getInAlluxioPercentage() != 100) {
      System.out.println("Skipping the file because the in alluxio pct isnt 100");
      return;
    }
    List<BlockLocationInfo> bl = mFileSystem.getBlockLocations(status);
    Map<WorkerNetAddress, List<Long>> blockIdMap = new HashMap<>();
    for (BlockLocationInfo blockLocationInfo: bl) {
      for (WorkerNetAddress address: blockLocationInfo.getLocations()) {
        if (!blockIdMap.containsKey(address)) {
          blockIdMap.put(address, new ArrayList<>());
        }
        blockIdMap.get(address).add(blockLocationInfo.getBlockInfo().getBlockInfo().getBlockId());
      }
    }
    Map<WorkerNetAddress, ListenableFuture<GetBlockChecksumResponse>> futures = new HashMap<>();
    for (Map.Entry<WorkerNetAddress, List<Long>> entry: blockIdMap.entrySet()) {
      try (CloseableResource<BlockWorkerClient> blockWorkerClient
               = mFsContext.acquireBlockWorkerClient(entry.getKey())) {
        GetBlockChecksumRequest request =
            GetBlockChecksumRequest.newBuilder().addAllBlockIds(entry.getValue()).addBlockIds(114514).build();
        futures.put(entry.getKey(), blockWorkerClient.get().getBlockChecksum(request));
      }
    }
    Map<Long, BlockChecksum> checksumMap = new HashMap<>();
    for (Map.Entry<WorkerNetAddress, ListenableFuture<GetBlockChecksumResponse>> entry:
        futures.entrySet()) {
      try {
        GetBlockChecksumResponse response = entry.getValue().get();
        for (Map.Entry<Long, BlockChecksum> checksumEntry: response.getChecksumMap().entrySet()) {
          long blockId = checksumEntry.getKey();
          BlockChecksum checksum = checksumEntry.getValue();
          if (checksumMap.containsKey(blockId)) {
            BlockChecksum checksumFromMap = checksumMap.get(blockId);
            if (checksumFromMap.getBlockLength() != checksum.getBlockLength()
                || !Objects.equals(checksumFromMap.getChecksum(), checksum.getChecksum())) {
              throw new RuntimeException("Block replica > 1 && the checksum does not match");
            }
          }
          checksumMap.put(blockId, checksum);
        }
      } catch (Exception e) {
        throw new RuntimeException("rpc call failed " + entry.getKey(), e);
      }
    }
    for (long blockId : status.getBlockIds()) {
      if (!checksumMap.containsKey(blockId)) {
        throw new RuntimeException("block does not exist");
      }
      BlockChecksum bcs = checksumMap.get(blockId);
      crc64Value = CRC64.combine(crc64Value, Long.parseLong(bcs.getChecksum()), bcs.getBlockLength());
    }
    System.out.println("CRC64 value from workers: " + Long.toHexString(crc64Value));
    long crc64ValueFromUfs =
        Long.parseLong(new String(ufsStatus.getXAttr().get(Constants.CRC64_KEY)));
    if (crc64Value != crc64ValueFromUfs) {
      System.out.println("Mismatch, data deleted from alluxio");
      mFileSystem.delete(plainPath, DeletePOptions.newBuilder().setAlluxioOnly(true).build());
    }
    System.out.println("check passed");
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    for (String dirArg : args) {
      AlluxioURI path = new AlluxioURI(dirArg);
      runPlainPath(path, cl);
    }
    return 0;
  }

  @Override
  public String getCommandName() {
    return "crc64check";
  }

  @Override
  public String getUsage() {
    return "foobar";
  }

  @Override
  public String getDescription() {
    return "barfoo";
  }
}
