// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/meta_master.proto

package alluxio.grpc;

public interface GetMasterInfoPOptionsOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.meta.GetMasterInfoPOptions)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>repeated .alluxio.grpc.meta.MasterInfoField filter = 1;</code>
   */
  java.util.List<alluxio.grpc.MasterInfoField> getFilterList();
  /**
   * <code>repeated .alluxio.grpc.meta.MasterInfoField filter = 1;</code>
   */
  int getFilterCount();
  /**
   * <code>repeated .alluxio.grpc.meta.MasterInfoField filter = 1;</code>
   */
  alluxio.grpc.MasterInfoField getFilter(int index);
}
