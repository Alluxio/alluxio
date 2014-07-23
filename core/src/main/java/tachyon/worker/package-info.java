/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Worker process and utils for working with the worker remotely.
 *
 * Main entry point for the worker is {@link tachyon.worker.TachyonWorker#main(String[])} which
 * gets started by the tachyon start scripts.  The {@link tachyon.worker.TachyonWorker} class
 * spins up the different RPC services (thrift, data) which are mostly wrappers around
 * {@link tachyon.worker.WorkerStorage}.
 *
 * <h1>Services</h1>
 *
 * <h2>Thrift</h2>
 *
 * The thrift service is mostly responsible for metadata operations (with a few operations
 * that effect the worker's cached memory).
 *
 * <h3>Checkpoint</h3>
 *
 * The act of moving temporary data into accessible data on {@link tachyon.UnderFileSystem}.  This
 * is triggered by {@link tachyon.client.WriteType#isThrough()} operations.
 *
 * Implementation can be found at {@link tachyon.worker.WorkerStorage#addCheckpoint(long, int)}
 *
 * <h3>Cache Block</h3>
 *
 * Move's user generated blocks to the tachyon data directory.  This operation expects that the
 * caller is a local (to the node) caller, and that the input are under the user directories.
 *
 * Implementation can be found at {@link tachyon.worker.WorkerStorage#cacheBlock(long, long)}
 *
 * <h3>Lock / Unlock</h3>
 *
 * Tachyon supports cacheing blocks to local disk (client side).  When this happens, a lock is
 * given to the client to protect it from the remote block changing.
 *
 * Implementation can be found at {@link tachyon.worker.WorkerStorage#lockBlock(long, long)}
 * and {@link tachyon.worker.WorkerStorage#unlockBlock(long, long)}.
 *
 */
package tachyon.worker;