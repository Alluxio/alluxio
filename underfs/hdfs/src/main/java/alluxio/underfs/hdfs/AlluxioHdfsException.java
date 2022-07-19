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
import alluxio.grpc.ErrorType;

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
   * Converts a Hdfs exception to a corresponding AlluxioHdfsException.
   *
   * @param e hdfs exception
   * @return alluxio hdfs exception
   */
  public static AlluxioHdfsException fromUfsException(Exception e) {
    e = convertException(e);
    if (e instanceof SecurityException) {
      return new AlluxioHdfsException(Status.PERMISSION_DENIED, e.getMessage(), e, ErrorType.User,
          false);
    }
    if (e instanceof AuthorizationException) {
      return new AlluxioHdfsException(Status.UNAUTHENTICATED, e.getMessage(), e, ErrorType.User,
          false);
    }
    if (e instanceof FileNotFoundException) {
      return new AlluxioHdfsException(Status.NOT_FOUND, e.getMessage(), e, ErrorType.User, false);
    }
    if (e instanceof UnsupportedOperationException) {
      return new AlluxioHdfsException(Status.UNIMPLEMENTED, e.getMessage(), e, ErrorType.User,
          false);
    }
    if (e instanceof IllegalArgumentException) {
      return new AlluxioHdfsException(Status.INVALID_ARGUMENT, e.getMessage(), e, ErrorType.User,
          false);
    }
    if (e instanceof IOException) {
      return new AlluxioHdfsException(Status.ABORTED, e.getMessage(), e, ErrorType.External, true);
    }
    return new AlluxioHdfsException(Status.UNKNOWN, e.getMessage(), e, ErrorType.External, false);
  }

  AlluxioHdfsException(Status status, String message, Throwable cause, ErrorType errorType,
      boolean retryable) {
    // Almost all HDFS exception are not retryable
    super(status, message, cause, errorType, retryable);
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
