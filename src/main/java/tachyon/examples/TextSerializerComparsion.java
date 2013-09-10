package tachyon.examples;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import tachyon.Version;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.InvalidPathException;

public class TextSerializerComparsion {
  public static String sInputFile;
  public static byte[] sData;

  public static void main(String[] args) 
      throws IOException, InvalidPathException, FileAlreadyExistException, ClassNotFoundException {
    if (args.length != 1) {
      System.out.println("java -cp target/tachyon-" + Version.VERSION + 
          "-jar-with-dependencies.jar tachyon.examples.TextSerializerComparsion " + "<FileName>\n");
      System.exit(-1);
    }
    sInputFile = args[0];
    getData(sInputFile);

    for (int k = 0; k < 1000000; k ++);
    long startTimeMs = System.currentTimeMillis();
    int cnt = TextPerf();
    System.out.println("Text Perf " + cnt + " took " + (System.currentTimeMillis() - startTimeMs) + " ms.");

    createJavaPerfData();
    createKryoPerfData();

    getData(sInputFile + ".java");

    for (int k = 0; k < 1000000; k ++);
    startTimeMs = System.currentTimeMillis();
    cnt = JavaPerf();
    System.out.println("Java Perf " + cnt + " took " + (System.currentTimeMillis() - startTimeMs) + " ms.");

    getData(sInputFile + ".kryo");

    for (int k = 0; k < 1000000; k ++);
    startTimeMs = System.currentTimeMillis();
    cnt = KryoPerf();
    System.out.println("Kryo Perf " + cnt + " took " + (System.currentTimeMillis() - startTimeMs) + " ms.");

    System.exit(0);
  }

  private static int KryoPerf() {
    System.out.println("Starting Kryo Perf.");
    Text text = new Text();

    ByteArrayInputStream is = new ByteArrayInputStream(sData);
    Kryo kryo = new Kryo();
    kryo.register(Text.class);
    Input input = new Input(is);

    int cnt = 0;
    while (input.canReadInt()) {
      text = kryo.readObject(input, Text.class);
      if (text == null) {
        break;
      }
      //      if (cnt < 30) {
      //        System.out.println(text);
      //      }
      cnt ++;
    }
    input.close();
    return cnt;
  }

  private static int JavaPerf() throws ClassNotFoundException {
    System.out.println("Starting Java Perf.");
    Text text = new Text();
    ByteArrayInputStream is = new ByteArrayInputStream(sData);
    ObjectInputStream ois = null;
    try {
      ois = new ObjectInputStream(is);
    } catch (IOException e1) {
      e1.printStackTrace();
    }
    int cnt = 0;
    while (true) {
      try {
        //        ois.defaultReadObject();
        ObjectWritable ow = new ObjectWritable();
        ow.setConf(new Configuration());
        ow.readFields(ois);
        text = ((Text) ow.get());

        //        if (cnt < 30) {
        //          System.out.println(text);
        //        }
      } catch (IOException e) {
        //        e.printStackTrace();
        break;
      }
      cnt ++;
    }
    return cnt;
  }

  private static void createKryoPerfData() throws IOException {
    System.out.println("CreateKryoPerfData.");
    Text text = new Text();

    Kryo kryo = new Kryo();
    kryo.register(Text.class);
    Output output = null;
    try {
      output = new Output(new FileOutputStream(sInputFile + ".kryo"));
    } catch (FileNotFoundException e1) {
      e1.printStackTrace();
    }

    ByteArrayInputStream is = new ByteArrayInputStream(sData);
    LineReader reader = new LineReader(is);
    while (true) {
      try {
        if (reader.readLine(text) == 0) {
          break;
        }
        kryo.writeObject(output, text);
      } catch (IOException e) {
        e.printStackTrace();
        break;
      }
    }
    reader.close();
    output.close();
  }

  private static void createJavaPerfData() throws FileNotFoundException, IOException {
    System.out.println("CreateJavaPerfData.");
    Text text = new Text();
    ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(sInputFile + ".java"));
    ByteArrayInputStream is = new ByteArrayInputStream(sData);
    LineReader reader = new LineReader(is);
    int cnt = 0;
    while (true) {
      try {
        if (reader.readLine(text) == 0) {
          break;
        }
        new ObjectWritable(text).write(oos);
      } catch (IOException e) {
        e.printStackTrace();
        break;
      }
      cnt ++;
    }
    reader.close();
    oos.close();
  }

  private static int TextPerf() throws IOException {
    System.out.println("Starting Text Perf.");
    Text text = new Text();
    ByteArrayInputStream is = new ByteArrayInputStream(sData);
    LineReader reader = new LineReader(is);
    int cnt = 0;
    while (true) {
      try {
        if (reader.readLine(text) == 0) {
          break;
        }
      } catch (IOException e) {
        System.out.println(e);
        break;
      }
      cnt ++;
    }
    reader.close();
    return cnt;
  }

  private static void getData(String file) throws IOException {
    System.out.println("Getting input data from file " + file);
    FileSystem fs = FileSystem.get(new Configuration());
    long length = fs.getLength(new Path(file));
    System.out.println("length = " + length);
    sData = new byte[(int) length];
    InputStream is = fs.open(new Path(file));
    int off = 0;
    int left = (int) length;
    while (left > 0) {
      int red = is.read(sData, off, left);
      off += red;
      left -= red;
    }
  }
}
