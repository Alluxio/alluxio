// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/block_master.proto

package alluxio.grpc;

public interface GetBlockMasterInfoPOptionsOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.block.GetBlockMasterInfoPOptions)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>repeated .alluxio.grpc.block.BlockMasterInfoField filters = 1;</code>
   * @return A list containing the filters.
   */
  java.util.List<alluxio.grpc.BlockMasterInfoField> getFiltersList();
  /**
   * <code>repeated .alluxio.grpc.block.BlockMasterInfoField filters = 1;</code>
   * @return The count of filters.
   */
  int getFiltersCount();
  /**
   * <code>repeated .alluxio.grpc.block.BlockMasterInfoField filters = 1;</code>
   * @param index The index of the element to return.
   * @return The filters at the given index.
   */
  alluxio.grpc.BlockMasterInfoField getFilters(int index);
}
