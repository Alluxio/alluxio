package tachyon.client.next.file;

import tachyon.TachyonURI;
import tachyon.client.FileInStream;
import tachyon.client.FileOutStream;
import tachyon.client.next.ClientOptions;
import tachyon.thrift.FileInfo;

import java.util.List;

public class TachyonFS {

  public static TachyonFS get() {
    // TODO: Implement me
    return null;
  }

  public void deleteFile(TachyonFile file) {
    // TODO: Implement me
  }

  public void freeFile(TachyonFile file) {
    // TODO: Implement me
  }

  public TachyonFile getFile(TachyonURI path) {
    // TODO: Implement me
    return null;
  }

  public FileInfo getFileInfo(TachyonFile file) {
    // TODO: Implement me
    return null;
  }

  public FileInStream getFileInStream(TachyonFile file, ClientOptions options) {
    // TODO: Implement me
    return null;
  }

  public FileOutStream getFileOutStream(TachyonFile file, ClientOptions options) {
    // TODO: Implement me
    return null;
  }

  public List<FileInfo> listStatus(TachyonFile file) {
    // TODO: Implement me
    return null;
  }

  public boolean mkdirs(TachyonFile file) {
    // TODO: Implement me
    return false;
  }

  public boolean promote(TachyonFile file) {
    // TODO: Implement me
    return false;
  }

  public boolean renameFile(TachyonFile file, TachyonURI dst) {
    // TODO: Implement me
    return false;
  }
}
