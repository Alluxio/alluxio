
/**
 * Worker process and utils for working with the worker remotely.
 *
 * Main entry point for the worker is {@link tachyon.worker.TachyonWorker#main(String[])} which
 * gets started by the tachyon start scripts. The {@link tachyon.worker.TachyonWorker} class
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
 * The act of moving temporary data into accessible data on {@link tachyon.UnderFileSystem}. This
 * is triggered by {@link tachyon.client.WriteType#isThrough()} operations.
 *
 * Implementation can be found at {@link tachyon.worker.WorkerStorage#addCheckpoint(long, int)}
 *
 * <h3>Cache Block</h3>
 *
 * Move's user generated blocks to the tachyon data directory. This operation expects that the
 * caller is a local (to the node) caller, and that the input are under the user directories.
 *
 * Implementation can be found at {@link tachyon.worker.WorkerStorage#cacheBlock(long, long)}
 *
 * <h3>Lock / Unlock</h3>
 *
 * Tachyon supports cacheing blocks to local disk (client side). When this happens, a lock is
 * given to the client to protect it from the remote block changing.
 *
 * Implementation can be found at {@link tachyon.worker.WorkerStorage#lockBlock(long, long)}
 * and {@link tachyon.worker.WorkerStorage#unlockBlock(long, long)}.
 *
 * <h2>Data</h2>
 *
 * This service is the main interaction between users and reading blocks. Currently this service
 * only supports reading blocks (writing is to local disk).
 *
 * {@link tachyon.worker.nio.NIODataServer} is the main entrypoint for the service to start and listen on.
 * The service is implemented with raw NIO.  Take a look at {@link tachyon.worker.nio.NIODataServer#run()}
 * to see what the server does, but to see what the data returned looks like, thats the
 * {@link tachyon.worker.nio.DataServerMessage}.  This has two different types: read
 * {@link tachyon.worker.nio.DataServerMessage#createBlockRequestMessage(long, long, long)} and write
 * {@link tachyon.worker.nio.DataServerMessage#createBlockResponseMessage(boolean, long, long, long)}
 *
 */
package tachyon.worker;
