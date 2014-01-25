package succinct.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

import tachyon.client.InStream;
import tachyon.client.TachyonByteBuffer;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.ReadType;


public class ReadData {


	private static TachyonFS tachyonClient;
	private static TachyonFile file;
	private static int fileID;
	private static InStream is;
	private static TachyonByteBuffer buf;
	private static int[] randoms;

	private static int BUFFER_SIZE = 1024;

	// Streaming Reads
	public static long streamReads() throws IOException {
		long read = 0;
		long startTimeStream, endTimeStream, timeStream = 0L;
		byte[] buffer = new byte[BUFFER_SIZE];
		startTimeStream = System.nanoTime();
		
		for(int j = 0; j < 1024; j++)
			for(int i = 0; i < randoms.length; i++)
				read += is.read(buffer);

		endTimeStream = System.nanoTime();
		timeStream += (endTimeStream - startTimeStream);
		
		System.out.println("Count = " + read);

		return timeStream;
	}

	// Buffer Reads
	public static long bufferReads() {
		long read = 0;
		long startTimeBuffer, endTimeBuffer, timeBuffer = 0L;
		
		// byte[] buffer = new byte[BUFFER_SIZE];
		buf.DATA.order(ByteOrder.nativeOrder());
		LongBuffer longBuf = buf.DATA.asLongBuffer();
		ByteBuffer bufData = buf.DATA;
		startTimeBuffer = System.nanoTime();

		for(int j = 0; j < 1024; j++)
			for(int i = 0; i < randoms.length; i++)
				read += bufData.get(randoms[i]);	

		endTimeBuffer = System.nanoTime();
		timeBuffer += (endTimeBuffer - startTimeBuffer);

		System.out.println("Count = " + read);

		return timeBuffer;
	}

	// public static long blockReads() {
	// 	long read = 0;
	// 	long startTimeBlocks, endTimeBlocks, timeBlocks = 0;

	// 	// startTimeBlocks = System.nanoTime();
	// 	for(int i = 0; i < randoms.length; i++) {
	// 		// tachyonClient.getBlockId(fileID, randoms[i]);
	// 		TachyonByteBuffer randBuf = file.readLocalByteBuffer(randoms[i]);
	// 		randBuf.DATA.order(ByteOrder.nativeOrder());
    // 		randBuf.close();
	// 	}


	// }

	public static void main(String[] args) throws IOException {

		if(args.length != 3) {
			System.out.println("Required 3 parameters: [masterAddress] [filePath] [numReads]");
			System.exit(1);
		}

		String masterAddress = args[0];
		String filePath = args[1];
		int numReads = Integer.parseInt(args[2]);
		java.util.Random rand = new java.util.Random();
		try {
			tachyonClient = TachyonFS.get(masterAddress);
			file = tachyonClient.getFile(filePath);
			fileID = tachyonClient.getFileId(filePath);
			buf = file.readByteBuffer();
			is = file.getInStream(ReadType.getOpType("NO_CACHE"));
			randoms = new int[numReads];
			for(int i = 0; i < Integer.parseInt(args[2]); i++) {
				randoms[i] = rand.nextInt((int)(file.length()));
				// System.out.println("random = " + randoms[i]);
			}
		} catch (Exception e) {
			System.out.println("Error! " + e.toString());
			System.exit(1);
		}

		// System.out.println("Testing Streaming Reads...");
		// long timeStream = streamReads();
		// System.out.println("Took " + (timeStream) + " ns to read " + (randoms.length * BUFFER_SIZE * 100) + " bytes");
		// double throughputStream = (double)(randoms.length * 1000.0 * 1000.0 * 1000.0 * 1024.0 * BUFFER_SIZE) / ((double)(timeStream) * 1024.0 * 1024.0);
		// System.out.println("Throughput = " + throughputStream + " MB/s");

		System.out.println("Testing Buffer Reads...");
		long timeBuffer = bufferReads();
		System.out.println("Took " + (timeBuffer) + " ns to read " + (randoms.length * 1024) + " bytes");
		double throughputBuffer = (double)(randoms.length * 1000.0 * 1000.0 * 1000.0 * 1024.0) / ((double)(timeBuffer) * 1024.0 * 1024.0);

		System.out.println("Throughput = " + throughputBuffer + " MB/s");

		buf.close();

		System.exit(0);
	}
}