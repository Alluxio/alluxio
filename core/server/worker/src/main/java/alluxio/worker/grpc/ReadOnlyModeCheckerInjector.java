/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.grpc;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Server side interceptor that is used to set Read Only Mode Lock.
 */
public final class ReadOnlyModeCheckerInjector implements ServerInterceptor {
  private static final Logger LOG = LoggerFactory.getLogger(ReadOnlyModeCheckerInjector.class);

  private static final int presumeSplitValue = 2;

  private final BlockWorkerClientServiceHandler mhandler;

  public ReadOnlyModeCheckerInjector(BlockWorkerClientServiceHandler hander) {
    mhandler = hander;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                                               Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    /**
     * For streaming calls, below will make sure authenticated user is injected prior to creating
     * the stream. If the call gets closed during authentication, the listener we return below
     * will not continue.
     */
    testReadOnlyModeLock(call, headers);

    /**
     * For non-streaming calls to server, below listener will be invoked in the same thread that is
     * serving the call.
     */
    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(
            next.startCall(call, headers)) {
      @Override
      public void onHalfClose() {
          testReadOnlyModeLock(call, headers);
          super.onHalfClose();
      }
    };
  }

  // TODO(Tony Sun): Make thread-safe.
  private <ReqT, RespT> void testReadOnlyModeLock(ServerCall<ReqT, RespT> call, Metadata headers) {
    System.out.println(call.getMethodDescriptor().getFullMethodName());
    String[] sArray = call.getMethodDescriptor().getFullMethodName().split("/");
    // TODO(Tony Sun): Maybe useless. The validation may have been checked.
    if (sArray.length != presumeSplitValue) {
      closeQuietly(call, Status.FAILED_PRECONDITION.withDescription(
              "Invalid Command."), headers);
    }
    System.out.println(sArray[1]);
    if ((sArray[1].equals("DecommissionWorker") || sArray[1].equals("CreateLocalBlock") ||
            sArray[1].equals("Load") || sArray[1].equals("WriteBlock")) && mhandler.getReadOnlyModeStatus()) {
      closeQuietly(call, Status.FAILED_PRECONDITION.withDescription(
              "worker has been set to Read Only Mode."), headers);
    }
  }

  /**
   * Closes the call while blanketing runtime exceptions. This is mostly to avoid dumping "already
   * closed" exceptions to logs.
   *
   * @param call call to close
   * @param status status to close the call with
   * @param headers headers to close the call with
   */
  private <ReqT, RespT> void closeQuietly(ServerCall<ReqT, RespT> call, Status status,
                                          Metadata headers) {
    try {
      LOG.debug("Closing the call:{} with Status:{}",
              call.getMethodDescriptor().getFullMethodName(), status);
      call.close(status, headers);
    } catch (RuntimeException exc) {
      LOG.debug("Error while closing the call:{} with Status:{}: {}",
              call.getMethodDescriptor().getFullMethodName(), status, exc);
    }
  }
}
