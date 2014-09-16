package tachyon.web;

import java.io.IOException;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.google.common.io.ByteStreams;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.master.BlockInfo;
import tachyon.master.MasterInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.util.CommonUtils;

/**
 * Servlet that provides data for browsing the file system.
 */
public class WebInterfaceBrowseServlet extends HttpServlet {
  /**
   * Class to make referencing file objects more intuitive. Mainly to avoid implicit association by
   * array indexes.
   */
  public static class UiBlockInfo implements Comparable<UiBlockInfo> {
    private final long mId;
    private final long mBlockLength;
    private final boolean mInMemory;

    public UiBlockInfo(BlockInfo blockInfo) {
      mId = blockInfo.mBlockId;
      mBlockLength = blockInfo.mLength;
      mInMemory = blockInfo.isInMemory();
    }

    @Override
    public int compareTo(UiBlockInfo p) {
      return (mId < p.mId ? -1 : (mId == p.mId ? 0 : 1));
    }

    public long getBlockLength() {
      return mBlockLength;
    }

    public long getID() {
      return mId;
    }

    public boolean inMemory() {
      return mInMemory;
    }
  }

  private static final long serialVersionUID = 6121623049981468871L;

  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private MasterInfo mMasterInfo;

  public WebInterfaceBrowseServlet(MasterInfo masterInfo) {
    mMasterInfo = masterInfo;
  }

  /**
   * This function displays 5KB of a file from a specific offset if it is in ASCII format.
   * 
   * @param path The path of the file to display
   * @param request The HttpServletRequest object
   * @param offset Where the file starts to display.
   * @throws FileDoesNotExistException
   * @throws IOException
   * @throws InvalidPathException
   */
  private void displayFile(String path, HttpServletRequest request, long offset)
      throws FileDoesNotExistException, InvalidPathException, IOException {
    String masterAddress =
        Constants.HEADER + mMasterInfo.getMasterAddress().getHostName() + ":"
            + mMasterInfo.getMasterAddress().getPort();
    TachyonFS tachyonClient = TachyonFS.get(new TachyonURI(masterAddress));
    TachyonFile tFile = tachyonClient.getFile(new TachyonURI(path));
    String fileData = null;
    if (tFile == null) {
      throw new FileDoesNotExistException(path);
    }
    if (tFile.isComplete()) {
      InStream is = tFile.getInStream(ReadType.NO_CACHE);
      int len = (int) Math.min(5 * Constants.KB, tFile.length() - offset);
      byte[] data = new byte[len];
      is.skip(offset);
      is.read(data, 0, len);
      fileData = CommonUtils.convertByteArrayToStringWithoutEscape(data);
      if (fileData == null) {
        fileData = "The requested file is not completely encoded in ascii";
      }
      is.close();
    } else {
      fileData = "The requested file is not complete yet.";
    }
    try {
      tachyonClient.close();
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
    List<BlockInfo> rawBlockList = mMasterInfo.getBlockList(path);
    List<UiBlockInfo> uiBlockInfo = new ArrayList<UiBlockInfo>();
    for (BlockInfo blockInfo : rawBlockList) {
      uiBlockInfo.add(new UiBlockInfo(blockInfo));
    }
    request.setAttribute("fileBlocks", uiBlockInfo);
    request.setAttribute("fileData", fileData);
  }

  /**
   * Populates attribute fields with data from the MasterInfo associated with this servlet. Errors
   * will be displayed in an error field. Debugging can be enabled to display additional data. Will
   * eventually redirect the request to a jsp.
   * 
   * @param request The HttpServletRequest object
   * @param response The HttpServletResponse object
   * @throws ServletException
   * @throws IOException
   * @throws UnknownHostException
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException, UnknownHostException {
    request.setAttribute("debug", Constants.DEBUG);

    request.setAttribute("masterNodeAddress", mMasterInfo.getMasterAddress().toString());
    request.setAttribute("invalidPathError", "");
    List<ClientFileInfo> filesInfo = null;
    String currentPath = request.getParameter("path");
    if (currentPath == null || currentPath.isEmpty()) {
      currentPath = Constants.PATH_SEPARATOR;
    }
    request.setAttribute("currentPath", currentPath);
    request.setAttribute("viewingOffset", 0);

    try {
      ClientFileInfo clientFileInfo = mMasterInfo.getClientFileInfo(currentPath);
      UiFileInfo currentFileInfo = new UiFileInfo(clientFileInfo);
      request.setAttribute("currentDirectory", currentFileInfo);
      request.setAttribute("blockSizeByte", currentFileInfo.getBlockSizeBytes());
      if (!currentFileInfo.getIsDirectory()) {
        String tmpParam = request.getParameter("offset");
        String opParam = request.getParameter("op");
        long offset = 0;
        try {
          if (tmpParam != null) {
            offset = Long.valueOf(tmpParam);
          }
        } catch (NumberFormatException nfe) {
          offset = 0;
        }
        if (offset < 0) {
          offset = 0;
        } else if (offset > clientFileInfo.getLength()) {
          offset = clientFileInfo.getLength();
        }
        if (opParam != null && "DOWNLOAD".equals(opParam)) {
          downloadFile(currentFileInfo.getAbsolutePath(), request, response);
        } else {
          displayFile(currentFileInfo.getAbsolutePath(), request, offset);
          request.setAttribute("viewingOffset", offset);
          getServletContext().getRequestDispatcher("/viewFile.jsp").forward(request, response);
        }
        return;
      }
      CommonUtils.validatePath(currentPath);
      setPathDirectories(currentPath, request);
      filesInfo = mMasterInfo.getFilesInfo(currentPath);
    } catch (FileDoesNotExistException fdne) {
      request.setAttribute("invalidPathError", "Error: Invalid Path " + fdne.getMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
      return;
    } catch (InvalidPathException ipe) {
      request.setAttribute("invalidPathError", "Error: Invalid Path " + ipe.getLocalizedMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
      return;
    } catch (IOException ie) {
      request.setAttribute("invalidPathError", "Error: File " + currentPath + " is not available "
          + ie.getMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
      return;
    }

    List<UiFileInfo> fileInfos = new ArrayList<UiFileInfo>(filesInfo.size());
    for (ClientFileInfo fileInfo : filesInfo) {
      UiFileInfo toAdd = new UiFileInfo(fileInfo);
      try {
        if (!toAdd.getIsDirectory() && fileInfo.getLength() > 0) {
          toAdd.setFileLocations(mMasterInfo.getFileBlocks(toAdd.getId()).get(0).getLocations());
        }
      } catch (FileDoesNotExistException fdne) {
        request.setAttribute("invalidPathError", "Error: Invalid Path " + fdne.getMessage());
        getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
        return;
      }
      fileInfos.add(toAdd);
    }
    Collections.sort(fileInfos);
    request.setAttribute("fileInfos", fileInfos);

    getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
  }

  /**
   * This function sets the fileinfos for folders that are in the path to the current directory.
   * 
   * @param path The path of the current directory.
   * @param request The HttpServletRequest object
   * @throws FileDoesNotExistException
   * @throws InvalidPathException
   */
  private void setPathDirectories(String path, HttpServletRequest request)
      throws FileDoesNotExistException, InvalidPathException {
    if (path.equals(Constants.PATH_SEPARATOR)) {
      request.setAttribute("pathInfos", new UiFileInfo[0]);
      return;
    }

    String[] splitPath = path.split(Constants.PATH_SEPARATOR);
    UiFileInfo[] pathInfos = new UiFileInfo[splitPath.length - 1];
    String currentPath = Constants.PATH_SEPARATOR;
    pathInfos[0] = new UiFileInfo(mMasterInfo.getClientFileInfo(currentPath));
    for (int i = 1; i < splitPath.length - 1; i ++) {
      currentPath = CommonUtils.concat(currentPath, splitPath[i]);
      pathInfos[i] = new UiFileInfo(mMasterInfo.getClientFileInfo(currentPath));
    }
    request.setAttribute("pathInfos", pathInfos);
  }

  /**
   * This function prepares for downloading a file.
   * 
   * @param path The path of the file to download
   * @param request The HttpServletRequest object
   * @param response The HttpServletResponse object
   * @throws FileDoesNotExistException
   * @throws InvalidPathException
   * @throws IOException
   */
  private void downloadFile(String path, HttpServletRequest request, HttpServletResponse response)
      throws FileDoesNotExistException, InvalidPathException, IOException {
    String masterAddress =
        Constants.HEADER + mMasterInfo.getMasterAddress().getHostName() + ":"
            + mMasterInfo.getMasterAddress().getPort();
    TachyonFS tachyonClient = TachyonFS.get(new TachyonURI(masterAddress));
    TachyonFile tFile = tachyonClient.getFile(new TachyonURI(path));
    if (tFile == null) {
      throw new FileDoesNotExistException(path);
    }
    long len = tFile.length();
    String fileName = new TachyonURI(path).getName();
    response.setContentType("application/octet-stream");
    if (len <= Integer.MAX_VALUE) {
      response.setContentLength((int) len);
    } else {
      response.addHeader("Content-Length", Long.toString(len));
    }
    response.addHeader("Content-Disposition", "attachment;filename=" + fileName);

    InStream is = null;
    ServletOutputStream out = null;
    try {
      is = tFile.getInStream(ReadType.NO_CACHE);
      out = response.getOutputStream();
      ByteStreams.copy(is, out);
    } finally {
      out.flush();
      out.close();
      is.close();
    }
    try {
      tachyonClient.close();
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
  }
}
