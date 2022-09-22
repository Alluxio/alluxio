package alluxio.master.file.meta.cross.cluster;

import alluxio.grpc.NetAddress;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.proto.journal.CrossCluster.MountList;
import alluxio.proto.journal.CrossCluster.RemovedMount;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Tracks the state of the local mounts for publishing them to other clusters.
 * Will only publish information about cross cluster mounts that are not read only.
 */
public class LocalMountState {

  private MountList.Builder mCurrentMountState;
  private final Consumer<MountList> mOnMountChange;
  private final Consumer<MountList> mBeforeMountAdd;

  /**
   * @param localClusterId the local cluster id
   * @param localAddresses list of local ip addresses
   * @param onMountChange function to call when mount state is changed
   * @param beforeMountAdd will be called before a new mount is added
   */
  public LocalMountState(String localClusterId, InetSocketAddress[] localAddresses,
                         Consumer<MountList> onMountChange, Consumer<MountList> beforeMountAdd) {
    mCurrentMountState = MountList.newBuilder().setClusterId(localClusterId).addAllAddresses(
        Arrays.stream(localAddresses).map(address ->
            NetAddress.newBuilder().setHost(address.getHostName()).setRpcPort(address.getPort())
                .build())
            .collect(Collectors.toList()));
    mOnMountChange = onMountChange;
    mBeforeMountAdd = beforeMountAdd;
  }

  private MountList.Builder generateNewMountInfo(MountInfo info) {
    MountList.Builder newMountState = mCurrentMountState.clone();
    newMountState.addMounts(info.toUfsInfo());
    String ufsPath = info.getUfsUri().toString();
    List<RemovedMount> updatedRemoved = new ArrayList<>(newMountState.getRemovedMountsCount());
    for (RemovedMount removed : newMountState.getRemovedMountsList()) {
      if (!removed.getUfsPath().startsWith(ufsPath)) {
        updatedRemoved.add(removed);
      }
    }
    newMountState.clearRemovedMounts();
    newMountState.addAllRemovedMounts(updatedRemoved);
    return newMountState;
  }

  /**
   * This should be called before a mount is added.
   * It may throw an exception if there is an issue with adding the mount.
   * @param info the mount info
   */
  public void beforeAddMount(MountInfo info) {
    // non-cross cluster mounts do not need to be tracked
    if (!info.getOptions().getCrossCluster()) {
      return;
    }
    mBeforeMountAdd.accept(generateNewMountInfo(info).build());
  }

  /**
   * Called when a new mount is added.
   * @param info the mount info
   */
  public void addMount(MountInfo info) {
    // other clusters don't need to know if we mount a read only mount, as
    // the local cluster will track this and subscribe to any intersecting
    // mounts at other clusters that are not read only
    if (!info.getOptions().getCrossCluster() || info.getOptions().getReadOnly()) {
      return;
    }
    mCurrentMountState = generateNewMountInfo(info);
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
            .setTime(System.currentTimeMillis()).setUfsPath(info.getUfsUri().toString()).build());
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
    mCurrentMountState.clearRemovedMounts().clearMounts();
  }
}
