package alluxio.logserver;

import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerRepository;
import org.apache.log4j.spi.LoggingEvent;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.ObjectInputStream;
import java.net.Socket;

public class AlluxioLog4jSocketNode implements Runnable {
  private Socket mSocket;
  private LoggerRepository mHierarchy;
  private ObjectInputStream mObjectInputStream;

  public AlluxioLog4jSocketNode(Socket socket, LoggerRepository hierarchy) {
    mSocket = socket;
    mHierarchy = hierarchy;
    try {
      mObjectInputStream = new ObjectInputStream(
          new BufferedInputStream(mSocket.getInputStream())
      );
    } catch (InterruptedIOException e) {
      Thread.currentThread().interrupt();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (RuntimeException e) {
      e.printStackTrace();
    }
  }
  @Override
  public void run() {
    LoggingEvent event;
    Logger remoteLogger;
    try {
      if (mObjectInputStream != null) {
        while (true) {
          event = (LoggingEvent) mObjectInputStream.readObject();
          remoteLogger = mHierarchy.getLogger(event.getLoggerName());
          if (event.getLevel().isGreaterOrEqual(remoteLogger.getEffectiveLevel())) {
            remoteLogger.callAppenders(event);
          }
        }
      }
    } catch (java.io.EOFException e) {
      e.printStackTrace();
    } catch (java.net.SocketException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (mObjectInputStream != null) {
        try {
          mObjectInputStream.close();
        } catch (Exception e) {

        }
        if (mSocket != null) {
          try {
            mSocket.close();
          } catch (IOException e) {

          }
        }
      }
    }
  }
}
