// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/messaging_transport.proto

package alluxio.grpc;

public interface TransportMessageOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.messaging.TransportMessage)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional .alluxio.grpc.messaging.MessagingRequestHeader requestHeader = 1;</code>
   * @return Whether the requestHeader field is set.
   */
  boolean hasRequestHeader();
  /**
   * <code>optional .alluxio.grpc.messaging.MessagingRequestHeader requestHeader = 1;</code>
   * @return The requestHeader.
   */
  alluxio.grpc.MessagingRequestHeader getRequestHeader();
  /**
   * <code>optional .alluxio.grpc.messaging.MessagingRequestHeader requestHeader = 1;</code>
   */
  alluxio.grpc.MessagingRequestHeaderOrBuilder getRequestHeaderOrBuilder();

  /**
   * <code>optional .alluxio.grpc.messaging.MessagingResponseHeader responseHeader = 2;</code>
   * @return Whether the responseHeader field is set.
   */
  boolean hasResponseHeader();
  /**
   * <code>optional .alluxio.grpc.messaging.MessagingResponseHeader responseHeader = 2;</code>
   * @return The responseHeader.
   */
  alluxio.grpc.MessagingResponseHeader getResponseHeader();
  /**
   * <code>optional .alluxio.grpc.messaging.MessagingResponseHeader responseHeader = 2;</code>
   */
  alluxio.grpc.MessagingResponseHeaderOrBuilder getResponseHeaderOrBuilder();

  /**
   * <code>optional bytes message = 3;</code>
   * @return Whether the message field is set.
   */
  boolean hasMessage();
  /**
   * <code>optional bytes message = 3;</code>
   * @return The message.
   */
  com.google.protobuf.ByteString getMessage();
}
