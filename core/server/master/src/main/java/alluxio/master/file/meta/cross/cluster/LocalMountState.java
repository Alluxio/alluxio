package alluxio.master.file.meta.cross.cluster;

import alluxio.grpc.MountList;
import alluxio.grpc.NetAddress;
import alluxio.grpc.RemovedMount;
import alluxio.master.file.meta.options.MountInfo;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Tracks the state of the local mounts for publishing them to other clusters.
 * Will only publish information about cross cluster mounts that are not read only.
 */
public class LocalMountState {

  private final MountList.Builder mCurrentMountState;
  private final Consumer<MountList> mOnMountChange;

  /**
   * @param localClusterId the local cluster id
   * @param localAddresses list of local ip addresses
   * @param onMountChange function to call when mount state is changed
   */
  public LocalMountState(String localClusterId, InetSocketAddress[] localAddresses,
                         Consumer<MountList> onMountChange) {
    mCurrentMountState = MountList.newBuilder().setClusterId(localClusterId).addAllAddresses(
        Arrays.stream(localAddresses).map(address ->
            NetAddress.newBuilder().setHost(address.getHostName()).setRpcPort(address.getPort())
                .build())
            .collect(Collectors.toList()));
    mOnMountChange = onMountChange;
  }

  /**
   * Called when a new mount is added.
   * @param info the mount info
   */
  public void addMount(MountInfo info) {
    if (!info.getOptions().getCrossCluster() || info.getOptions().getReadOnly()) {
      return;
    }
    mCurrentMountState.addMounts(info.toUfsInfo());
    String ufsPath = info.getUfsUri().toString();
    List<RemovedMount> updatedRemoved = new ArrayList<>(mCurrentMountState.getRemovedMountsCount());
    for (RemovedMount removed : mCurrentMountState.getRemovedMountsList()) {
      if (!removed.getUfsPath().startsWith(ufsPath)) {
        updatedRemoved.add(removed);
      }
    }
    mCurrentMountState.addAllRemovedMounts(updatedRemoved);
    mOnMountChange.accept(mCurrentMountState.build());
  }

  /**
   * Called when removing an existing mount.
   * @param info the mount info
   */
  public void removeMount(MountInfo info) {
    if (!info.getOptions().getCrossCluster() || info.getOptions().getReadOnly()) {
      return;
    }
    for (int i = 0; i < mCurrentMountState.getMountsCount(); i++) {
      if (mCurrentMountState.getMounts(i).getUri().equals(info.getUfsUri().toString())) {
        mCurrentMountState.removeMounts(i);
        mCurrentMountState.addRemovedMounts(RemovedMount.newBuilder()
            .setTime(System.currentTimeMillis()).setUfsPath(info.getUfsUri().getPath()).build());
        mOnMountChange.accept(mCurrentMountState.build());
        return;
      }
    }
    throw new IllegalStateException("Tried to remove non existing mount" + info);
  }

  /**
   * Reset the local mount state (keeps information about local addresses and
   * local cluster id).
   */
  public void resetState() {
    mCurrentMountState.addAllRemovedMounts(Collections.emptyList())
        .addAllMounts(Collections.emptyList());
  }
}
