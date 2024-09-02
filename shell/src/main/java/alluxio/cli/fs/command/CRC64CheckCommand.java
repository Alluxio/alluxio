package alluxio.cli.fs.command;

import alluxio.Constants;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.grpc.BlockChecksum;
import alluxio.grpc.BlockLocation;
import alluxio.grpc.GetBlockChecksumRequest;
import alluxio.grpc.GetBlockChecksumResponse;
import alluxio.grpc.GetStatusPOptions;
import alluxio.resource.CloseableResource;
import alluxio.util.CRC64;
import alluxio.wire.BlockLocationInfo;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class CRC64CheckCommand extends AbstractFileSystemCommand{

  void aaa() throws IOException, AlluxioException, ExecutionException, InterruptedException {
    long crc64Value = 0;

    URIStatus status = mFileSystem.getStatus("FILE_PATH");
    List<BlockLocationInfo> bl = mFileSystem.getBlockLocations(status);
    CloseableResource<BlockWorkerClient> blockWorkerClient =
        mFsContext.acquireBlockWorkerClient(bl.get(0).getLocations().get(0));
    ListenableFuture<GetBlockChecksumResponse> f = blockWorkerClient.get().getBlockChecksum(GetBlockChecksumRequest.getDefaultInstance());
    ListenableFuture<List<GetBlockChecksumResponse>> ff = Futures.allAsList(f);
    //if (status.getInAlluxioPercentage() != 100)
    Map<Long, BlockChecksum> combinedMap = ff.get().stream()
        .flatMap(proto -> proto.getChecksumMap().entrySet().stream())
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            Map.Entry::getValue,
            (oldValue, newValue) -> oldValue));
    for (long blockId : status.getBlockIds()) {
      if (!combinedMap.containsKey(blockId)) {
        throw new RuntimeException("block does not exist");
      }
      BlockChecksum bcs = combinedMap.get(blockId);
      crc64Value = CRC64.combine(crc64Value, Long.parseLong(bcs.getChecksum()), bcs.getBlockLength());
    }
    URIStatus ufsStatus = mFileSystem.getStatus(
        "FILE_PATH", GetStatusPOptions.newBuilder().setDirectUfsAccess(true).build());

    long crc64ValueFromUfs =
        Long.parseLong(new String(ufsStatus.getXAttr().get(Constants.CRC64_KEY)));
    if (crc64Value != crc64ValueFromUfs) {
      throw new RuntimeException("Mismatch!");
    }
  }
}
