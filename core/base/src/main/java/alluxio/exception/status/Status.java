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

package alluxio.exception.status;

/**
 * A class representing gRPC status codes. The definitions are from
 * https://github.com/grpc/grpc-go/blob/v1.2.0/codes/codes.go.
 */
public enum Status {
  // OK is returned on success.
  OK,

  // Canceled indicates the operation was cancelled (typically by the caller).
  CANCELED,

  // Unknown error.  An example of where this error may be returned is
  // if a Status value received from another address space belongs to
  // an error-space that is not known in this address space.  Also
  // errors raised by APIs that do not return enough error information
  // may be converted to this error.
  UNKNOWN,

  // InvalidArgument indicates client specified an invalid argument.
  // Note that this differs from FailedPrecondition. It indicates arguments
  // that are problematic regardless of the state of the system
  // (e.g., a malformed file name).
  INVALID_ARGUMENT,

  // DeadlineExceeded means operation expired before completion.
  // For operations that change the state of the system, this error may be
  // returned even if the operation has completed successfully. For
  // example, a successful response from a server could have been delayed
  // long enough for the deadline to expire.
  DEADLINE_EXCEEDED,

  // NotFound means some requested entity (e.g., file or directory) was
  // not found.
  NOT_FOUND,

  // AlreadyExists means an attempt to create an entity failed because one
  // already exists.
  ALREADY_EXISTS,

  // PermissionDenied indicates the caller does not have permission to
  // execute the specified operation. It must not be used for rejections
  // caused by exhausting some resource (use ResourceExhausted
  // instead for those errors).  It must not be
  // used if the caller cannot be identified (use Unauthenticated
  // instead for those errors).
  PERMISSION_DENIED,

  // Unauthenticated indicates the request does not have valid
  // authentication credentials for the operation.
  UNAUTHENTICATED,

  // ResourceExhausted indicates some resource has been exhausted, perhaps
  // a per-user quota, or perhaps the entire file system is out of space.
  RESOURCE_EXHAUSTED,

  // FailedPrecondition indicates operation was rejected because the
  // system is not in a state required for the operation's execution.
  // For example, directory to be deleted may be non-empty, an rmdir
  // operation is applied to a non-directory, etc.
  //
  // A litmus test that may help a service implementor in deciding
  // between FailedPrecondition, Aborted, and Unavailable:
  //  (a) Use Unavailable if the client can retry just the failing call.
  //  (b) Use Aborted if the client should retry at a higher-level
  //      (e.g., restarting a read-modify-write sequence).
  //  (c) Use FailedPrecondition if the client should not retry until
  //      the system state has been explicitly fixed.  E.g., if an "rmdir"
  //      fails because the directory is non-empty, FailedPrecondition
  //      should be returned since the client should not retry unless
  //      they have first fixed up the directory by deleting files from it.
  //  (d) Use FailedPrecondition if the client performs conditional
  //      REST Get/Update/Delete on a resource and the resource on the
  //      server does not match the condition. E.g., conflicting
  //      read-modify-write on the same resource.
  FAILED_PRECONDITION,

  // Aborted indicates the operation was aborted, typically due to a
  // concurrency issue like sequencer check failures, transaction aborts,
  // etc.
  //
  // See litmus test above for deciding between FailedPrecondition,
  // Aborted, and Unavailable.
  ABORTED,

  // OutOfRange means operation was attempted past the valid range.
  // E.g., seeking or reading past end of file.
  //
  // Unlike InvalidArgument, this error indicates a problem that may
  // be fixed if the system state changes. For example, a 32-bit file
  // system will generate InvalidArgument if asked to read at an
  // offset that is not in the range [0,2^32-1], but it will generate
  // OutOfRange if asked to read from an offset past the current
  // file size.
  //
  // There is a fair bit of overlap between FailedPrecondition and
  // OutOfRange.  We recommend using OutOfRange (the more specific
  // error) when it applies so that callers who are iterating through
  // a space can easily look for an OutOfRange error to detect when
  // they are done.
  OUT_OF_RANGE,

  // Unimplemented indicates operation is not implemented or not
  // supported/enabled in this service.
  UNIMPLEMENTED,

  // Internal errors.  Means some invariants expected by underlying
  // system has been broken.  If you see one of these errors,
  // something is very broken.
  INTERNAL,

  // Unavailable indicates the service is currently unavailable.
  // This is a most likely a transient condition and may be corrected
  // by retrying with a backoff.
  //
  // See litmus test above for deciding between FailedPrecondition,
  // Aborted, and Unavailable.
  UNAVAILABLE,

  // DataLoss indicates unrecoverable data loss or corruption.
  DATA_LOSS;
}
