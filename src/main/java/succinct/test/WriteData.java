package succinct.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import tachyon.client.OutStream;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;

public class WriteData {

	public static void writeData(String masterAddress, String filePath, String writeType, int dataSize) {
		try {
			TachyonFS tachyonClient = TachyonFS.get(masterAddress);
			int fileID = tachyonClient.createFile(filePath);
			ByteBuffer buf = ByteBuffer.allocate(dataSize * 4);
			buf.order(ByteOrder.nativeOrder());
			for(int i = 0; i < dataSize; i++) {
				buf.putInt(i);
			}

			TachyonFile file = tachyonClient.getFile(filePath);
			OutStream os = file.getOutStream(WriteType.getOpType(writeType));
			os.write(buf.array());
			os.close();
		} catch(Exception e) {
			System.out.println("Error! " + e.toString());
		}
	}

	public static void main(String[] args) {
		if(args.length != 4) {
			System.out.println("Required 4 parameters: [masterAddress] [filePath] [writeType] [dataSize]");
			System.exit(1);
		}
		writeData(args[0], args[1], args[2], Integer.parseInt(args[3]));
		System.out.println("Wrote " + args[3] + " integers to file " + args[1] + "!");
		System.exit(0);
	}
}