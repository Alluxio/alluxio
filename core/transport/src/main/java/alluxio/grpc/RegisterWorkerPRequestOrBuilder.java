// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/block_master.proto

package alluxio.grpc;

public interface RegisterWorkerPRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.block.RegisterWorkerPRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <pre>
   ** the id of the worker 
   * </pre>
   *
   * <code>optional int64 workerId = 1;</code>
   */
  boolean hasWorkerId();
  /**
   * <pre>
   ** the id of the worker 
   * </pre>
   *
   * <code>optional int64 workerId = 1;</code>
   */
  long getWorkerId();

  /**
   * <pre>
   ** the list of storage tiers 
   * </pre>
   *
   * <code>repeated string storageTiers = 2;</code>
   */
  java.util.List<java.lang.String>
      getStorageTiersList();
  /**
   * <pre>
   ** the list of storage tiers 
   * </pre>
   *
   * <code>repeated string storageTiers = 2;</code>
   */
  int getStorageTiersCount();
  /**
   * <pre>
   ** the list of storage tiers 
   * </pre>
   *
   * <code>repeated string storageTiers = 2;</code>
   */
  java.lang.String getStorageTiers(int index);
  /**
   * <pre>
   ** the list of storage tiers 
   * </pre>
   *
   * <code>repeated string storageTiers = 2;</code>
   */
  com.google.protobuf.ByteString
      getStorageTiersBytes(int index);

  /**
   * <pre>
   ** the map of total bytes on each tier 
   * </pre>
   *
   * <code>map&lt;string, int64&gt; totalBytesOnTiers = 3;</code>
   */
  int getTotalBytesOnTiersCount();
  /**
   * <pre>
   ** the map of total bytes on each tier 
   * </pre>
   *
   * <code>map&lt;string, int64&gt; totalBytesOnTiers = 3;</code>
   */
  boolean containsTotalBytesOnTiers(
      java.lang.String key);
  /**
   * Use {@link #getTotalBytesOnTiersMap()} instead.
   */
  @java.lang.Deprecated
  java.util.Map<java.lang.String, java.lang.Long>
  getTotalBytesOnTiers();
  /**
   * <pre>
   ** the map of total bytes on each tier 
   * </pre>
   *
   * <code>map&lt;string, int64&gt; totalBytesOnTiers = 3;</code>
   */
  java.util.Map<java.lang.String, java.lang.Long>
  getTotalBytesOnTiersMap();
  /**
   * <pre>
   ** the map of total bytes on each tier 
   * </pre>
   *
   * <code>map&lt;string, int64&gt; totalBytesOnTiers = 3;</code>
   */

  long getTotalBytesOnTiersOrDefault(
      java.lang.String key,
      long defaultValue);
  /**
   * <pre>
   ** the map of total bytes on each tier 
   * </pre>
   *
   * <code>map&lt;string, int64&gt; totalBytesOnTiers = 3;</code>
   */

  long getTotalBytesOnTiersOrThrow(
      java.lang.String key);

  /**
   * <pre>
   ** the map of used bytes on each tier 
   * </pre>
   *
   * <code>map&lt;string, int64&gt; usedBytesOnTiers = 4;</code>
   */
  int getUsedBytesOnTiersCount();
  /**
   * <pre>
   ** the map of used bytes on each tier 
   * </pre>
   *
   * <code>map&lt;string, int64&gt; usedBytesOnTiers = 4;</code>
   */
  boolean containsUsedBytesOnTiers(
      java.lang.String key);
  /**
   * Use {@link #getUsedBytesOnTiersMap()} instead.
   */
  @java.lang.Deprecated
  java.util.Map<java.lang.String, java.lang.Long>
  getUsedBytesOnTiers();
  /**
   * <pre>
   ** the map of used bytes on each tier 
   * </pre>
   *
   * <code>map&lt;string, int64&gt; usedBytesOnTiers = 4;</code>
   */
  java.util.Map<java.lang.String, java.lang.Long>
  getUsedBytesOnTiersMap();
  /**
   * <pre>
   ** the map of used bytes on each tier 
   * </pre>
   *
   * <code>map&lt;string, int64&gt; usedBytesOnTiers = 4;</code>
   */

  long getUsedBytesOnTiersOrDefault(
      java.lang.String key,
      long defaultValue);
  /**
   * <pre>
   ** the map of used bytes on each tier 
   * </pre>
   *
   * <code>map&lt;string, int64&gt; usedBytesOnTiers = 4;</code>
   */

  long getUsedBytesOnTiersOrThrow(
      java.lang.String key);

  /**
   * <pre>
   ** the map of list of blocks on each tier 
   * </pre>
   *
   * <code>map&lt;string, .alluxio.grpc.block.TierList&gt; currentBlocksOnTiers = 5;</code>
   */
  int getCurrentBlocksOnTiersCount();
  /**
   * <pre>
   ** the map of list of blocks on each tier 
   * </pre>
   *
   * <code>map&lt;string, .alluxio.grpc.block.TierList&gt; currentBlocksOnTiers = 5;</code>
   */
  boolean containsCurrentBlocksOnTiers(
      java.lang.String key);
  /**
   * Use {@link #getCurrentBlocksOnTiersMap()} instead.
   */
  @java.lang.Deprecated
  java.util.Map<java.lang.String, alluxio.grpc.TierList>
  getCurrentBlocksOnTiers();
  /**
   * <pre>
   ** the map of list of blocks on each tier 
   * </pre>
   *
   * <code>map&lt;string, .alluxio.grpc.block.TierList&gt; currentBlocksOnTiers = 5;</code>
   */
  java.util.Map<java.lang.String, alluxio.grpc.TierList>
  getCurrentBlocksOnTiersMap();
  /**
   * <pre>
   ** the map of list of blocks on each tier 
   * </pre>
   *
   * <code>map&lt;string, .alluxio.grpc.block.TierList&gt; currentBlocksOnTiers = 5;</code>
   */

  alluxio.grpc.TierList getCurrentBlocksOnTiersOrDefault(
      java.lang.String key,
      alluxio.grpc.TierList defaultValue);
  /**
   * <pre>
   ** the map of list of blocks on each tier 
   * </pre>
   *
   * <code>map&lt;string, .alluxio.grpc.block.TierList&gt; currentBlocksOnTiers = 5;</code>
   */

  alluxio.grpc.TierList getCurrentBlocksOnTiersOrThrow(
      java.lang.String key);

  /**
   * <code>optional .alluxio.grpc.block.RegisterWorkerPOptions options = 6;</code>
   */
  boolean hasOptions();
  /**
   * <code>optional .alluxio.grpc.block.RegisterWorkerPOptions options = 6;</code>
   */
  alluxio.grpc.RegisterWorkerPOptions getOptions();
  /**
   * <code>optional .alluxio.grpc.block.RegisterWorkerPOptions options = 6;</code>
   */
  alluxio.grpc.RegisterWorkerPOptionsOrBuilder getOptionsOrBuilder();
}
