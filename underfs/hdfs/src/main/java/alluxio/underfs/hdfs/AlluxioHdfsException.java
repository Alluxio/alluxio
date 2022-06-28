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

package alluxio.underfs.hdfs;

import alluxio.exception.AlluxioRuntimeException;

import com.google.protobuf.Any;
import com.sun.jersey.api.ParamException;
import com.sun.jersey.api.container.ContainerException;
import io.grpc.Status;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.token.SecretManager;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Alluxio exception for HDFS. Borrow from webhdfs located at
 * org/apache/hadoop/hdfs/web/resources/ExceptionHandler.java
 */
public class AlluxioHdfsException extends AlluxioRuntimeException {

  /**
   * Converts an AmazonClientException to a corresponding AlluxioHdfsException.
   *
   * @param cause hdfs exception
   * @return alluxio hdfs exception
   */
  public static AlluxioHdfsException from(Exception cause) {
    cause = convertException(cause);
    Status status = getStatus(cause);
    // Almost all HDFS exception are not retryable
    return new AlluxioHdfsException(status, cause.getMessage(), cause, false);
  }

  private AlluxioHdfsException(Status status, String message, Throwable cause, boolean isRetryAble,
      Any... details) {
    super(status, message, cause, isRetryAble, details);
  }

  private static Status getStatus(Exception e) {
    //Map response status
    final Status s;
    if (e instanceof SecurityException) {
      s = Status.PERMISSION_DENIED;
    } else if (e instanceof AuthorizationException) {
      s = Status.UNAUTHENTICATED;
    } else if (e instanceof FileNotFoundException) {
      s = Status.NOT_FOUND;
    } else if (e instanceof IOException) {
      s = Status.ABORTED;
    } else if (e instanceof UnsupportedOperationException) {
      s = Status.UNIMPLEMENTED;
    } else if (e instanceof IllegalArgumentException) {
      s = Status.INVALID_ARGUMENT;
    } else {
      s = Status.UNKNOWN;
    }
    return s;
  }

  private static Exception convertException(Exception e) {
    if (e instanceof ParamException) {
      final ParamException paramexception = (ParamException) e;
      e = new IllegalArgumentException(
          "Invalid value for hdfs parameter \"" + paramexception.getParameterName() + "\": "
              + e.getCause().getMessage(), e);
    }
    if (e instanceof ContainerException) {
      e = toCause(e);
    }
    if (e instanceof RemoteException) {
      e = ((RemoteException) e).unwrapRemoteException();
    }

    if (e instanceof SecurityException) {
      e = toCause(e);
    }
    return e;
  }

  private static Exception toCause(Exception e) {
    final Throwable t = e.getCause();
    if (e instanceof SecurityException) {
      // For the issue reported in HDFS-6475, if SecurityException's cause
      // is InvalidToken, and the InvalidToken's cause is StandbyException,
      // return StandbyException; Otherwise, leave the exception as is,
      // since they are handled elsewhere. See HDFS-6588.
      if (t instanceof SecretManager.InvalidToken) {
        final Throwable t1 = t.getCause();
        if (t1 instanceof StandbyException) {
          e = (StandbyException) t1;
        }
      }
    } else {
      if (t instanceof Exception) {
        e = (Exception) t;
      }
    }
    return e;
  }
}
