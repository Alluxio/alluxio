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

package alluxio.resource;

/**
 * {@link SharableResource} is a marker interface for sharable resources.
 * Such resource can be safely pooled in a {@link SharedResourcePool} without
 * compromising correctness of the system.
 * <p></p>
 * The requirements on a sharable resource is that:
 * 1. Thread safe
 * 2. The correct working of its public API methods don't depend on each other, i.e., the public
 * methods should have orthogonal functionalities. This is a slightly lighter requirements than
 * "stateless": a purely stateless class is sharable, but statelessness is not strictly required.
 * For example: a thread-safe Counter class that provides "increment" and "get" APIs are sharable by
 * this standard. It maintains internal states, but the correctness promise of its public APIs don't
 * rely on any ordering of method calls.
 * Also, {@link io.grpc.ManagedChannel} is another example of sharable resources.
 * <p></p>
 * This is only a marker, and it is the implementor's responsibility to make sure the constraints
 * are satisfied.
 * <p></p>
 * Examples of in-use sharable resources include various kinds of {@link alluxio.Client} in alluxio,
 * which serve as functional layers on top of the grpc communication channel.
 */
public interface SharableResource {
}
