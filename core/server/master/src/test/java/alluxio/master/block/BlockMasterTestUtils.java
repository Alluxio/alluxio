package alluxio.master.block;

import alluxio.exception.BlockInfoException;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.WorkerInfo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class BlockMasterTestUtils {
  public static void verifyBlockOnWorkers(BlockMaster blockMaster, long blockId, long blockLength,
                                    List<WorkerInfo> workers) throws Exception {
    BlockInfo blockInfo = blockMaster.getBlockInfo(blockId);
    assertEquals(blockLength, blockInfo.getLength());
    assertEquals(workers.size(), blockInfo.getLocations().size());

    List<BlockLocation> expectedLocations = new ArrayList<>();
    for (WorkerInfo w : workers) {
      expectedLocations.add(new BlockLocation()
              .setWorkerAddress(w.getAddress())
              .setWorkerId(w.getId())
              .setMediumType("MEM")
              .setTierAlias("MEM"));
    }

    assertEquals(blockLength, blockInfo.getLength());
    assertEquals(expectedLocations.size(), blockInfo.getLocations().size());
    assertEquals(new HashSet<>(expectedLocations), new HashSet<>(blockInfo.getLocations()));
  }

  public static void verifyBlockNotExisting(BlockMaster blockMaster, long blockId) {
    assertThrows(BlockInfoException.class, () -> {
      blockMaster.getBlockInfo(blockId);
    });
  }

  public static WorkerInfo findWorkerInfo(List<WorkerInfo> list, long workerId) {
    for (WorkerInfo worker : list) {
      if (workerId == worker.getId()) {
        return worker;
      }
    }
    throw new AssertionError(String.format(
            "Failed to find workerId %s in the worker list %s", workerId, list));
  }
}
