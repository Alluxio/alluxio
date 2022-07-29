package alluxio.master.file.meta.crosscluster;

import alluxio.grpc.MountList;

import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks the cross cluster state at the configuration process.
 */
public class CrossClusterState {

  private final ConcurrentHashMap<String, MountList> mMounts =
      new ConcurrentHashMap<>();
  private final Map<String, StreamObserver<MountList>> mStreams = new ConcurrentHashMap<>();

  /**
   * Set the mount list for a cluster.
   * @param mountList the mount list
   */
  public synchronized void setMountList(MountList mountList) {
    MountList prevMountList = mMounts.get(mountList.getClusterId());
    if (prevMountList != null) {
      if (prevMountList.equals(mountList)) {
        return;
      }
    }
    mMounts.put(mountList.getClusterId(), mountList);
    mStreams.forEach((clusterId, stream) -> {
      if (!clusterId.equals(mountList.getClusterId())) {
        stream.onNext(mountList);
      }
    });
  }

  /**
   * Set the stream for the given cluster id.
   * @param clusterId the cluster id
   * @param stream the stream
   */
  public synchronized void setStream(String clusterId, StreamObserver<MountList> stream) {
    mStreams.compute(clusterId, (key, oldStream) -> {
      if (oldStream != null) {
        oldStream.onCompleted();
      }
      return stream;
    });
    mMounts.forEach((otherClusterId, mountList) -> {
      if (!clusterId.equals(otherClusterId)) {
        stream.onNext(mountList);
      }
    });
  }
}
