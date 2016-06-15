package alluxio.examples;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.AlluxioException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;


public class LoadGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

    private static class RandomString {

        private static final char[] symbols;

        static {
            StringBuilder tmp = new StringBuilder();
            for (char ch = '0'; ch <= '9'; ++ch)
                tmp.append(ch);
            for (char ch = 'a'; ch <= 'z'; ++ch)
                tmp.append(ch);
            symbols = tmp.toString().toCharArray();
        }

        private final Random random = new Random();

        private final char[] buf;

        RandomString(int length) {
            if (length < 1)
                throw new IllegalArgumentException("length < 1: " + length);
            buf = new char[length];
        }

        String nextString() {
            buf[0] = '/';
            for (int idx = 1; idx < buf.length; ++idx)
                buf[idx] = symbols[random.nextInt(symbols.length)];
            return new String(buf);
        }
    }

    public static void main(String[] args){
        AlluxioURI file;
        FileOutStream out;
        CreateFileOptions options = CreateFileOptions.defaults();
        int filenumber = 8000000;
        if(args.length == 1) {

            filenumber = Integer.parseInt(args[0]);
        }

        System.out.printf("start, default file number %d\n", filenumber);
        FileSystem fs = FileSystem.Factory.get();
        RandomString fileNameGenerator = new RandomString(6);
        try {
            for(int i=0; i<filenumber; i++) {
                file = new AlluxioURI(fileNameGenerator.nextString()+"A"+Integer.toString(i));

                out = fs.createFile(file, options);
                out.close();

                if(i%500000==0) {
                    System.out.printf("File Number: %d, Press enter to continue\n", i);
                }
            }
        } catch (IOException | AlluxioException e) {
            e.printStackTrace();
        }
    }
}




