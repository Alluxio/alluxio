package tachyon.web;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.io.IOException;
import java.net.UnknownHostException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import tachyon.CommonUtils;
import tachyon.Constants;
import tachyon.MasterInfo;
import tachyon.client.InStream;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.OpType;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.NetAddress;

/**
 * Servlet that provides data for browsing the file system.
 */
public class WebInterfaceBrowseServlet extends HttpServlet {
  private static final long serialVersionUID = 6121623049981468871L;
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private MasterInfo mMasterInfo;

  /**
   * Class to make referencing file objects more intuitive. Mainly to avoid implicit association
   * by array indexes.
   */
  public class UiFileInfo implements Comparable<UiFileInfo> {
    private final int ID;
    private final String NAME;
    private final String ABSOLUATE_PATH;
    private final String CHECKPOINT_PATH;
    private final long SIZE;
    private final long CREATION_TIME_MS;
    private final boolean IN_MEMORY;
    private final boolean IS_DIRECTORY;
    private List<String> mFileLocations;

    private UiFileInfo(ClientFileInfo fileInfo) {
      ID = fileInfo.getId();
      NAME = fileInfo.getName();
      ABSOLUATE_PATH = fileInfo.getPath();
      CHECKPOINT_PATH = fileInfo.getCheckpointPath();
      SIZE = fileInfo.getSizeBytes();
      CREATION_TIME_MS = fileInfo.getCreationTimeMs();
      IN_MEMORY = fileInfo.isInMemory();
      IS_DIRECTORY = fileInfo.isFolder();
      mFileLocations = new ArrayList<String>();
    }

    public int getId() {
      return ID;
    }

    public String getName() {
      if (ABSOLUATE_PATH.equals(Constants.PATH_SEPARATOR)) {
        return "root";
      } else {
        return NAME;
      }
    }

    public String getAbsolutePath() {
      return ABSOLUATE_PATH;
    }

    public String getCheckpointPath() {
      return CHECKPOINT_PATH;
    }

    public String getSize() {
      if (IS_DIRECTORY) {
        return " ";
      } else {
        return CommonUtils.getSizeFromBytes(SIZE);
      }
    }

    public String getCreationTime() {
      return CommonUtils.convertMsToDate(CREATION_TIME_MS);
    }

    public boolean getInMemory() {
      return IN_MEMORY;
    }

    public boolean getIsDirectory() {
      return IS_DIRECTORY;
    }

    public void setFileLocations(List<NetAddress> fileLocations) {
      for (NetAddress addr : fileLocations) {
        mFileLocations.add(new String(addr.getMHost() + ":" + addr.getMPort()));
      }
    }

    public List<String> getFileLocations() {
      return mFileLocations;
    }

    @Override
    public int compareTo(UiFileInfo o) {
      return ABSOLUATE_PATH.compareTo(o.getAbsolutePath());
    }
  }

  public WebInterfaceBrowseServlet(MasterInfo masterInfo) {
    mMasterInfo = masterInfo;
  }

  /**
   * Populates attribute fields with data from the MasterInfo associated with this servlet. Errors
   * will be displayed in an error field. Debugging can be enabled to display additional data.
   * Will eventually redirect the request to a jsp.
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

    if (currentPath.isEmpty()) {
      currentPath = "/";
    }
    request.setAttribute("currentPath", currentPath);
    try {
      UiFileInfo currentFileInfo = new UiFileInfo(mMasterInfo.getFileInfo(currentPath));
      request.setAttribute("currentDirectory", currentFileInfo);
      if (!currentFileInfo.getIsDirectory()) {
        displayFile(currentFileInfo.getAbsolutePath(), request);
        getServletContext().getRequestDispatcher("/viewFile.jsp").forward(request, response);
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
    }

    List<UiFileInfo> fileInfos = new ArrayList<UiFileInfo>(filesInfo.size());
    for (ClientFileInfo fileInfo : filesInfo) {
      UiFileInfo toAdd = new UiFileInfo(fileInfo);
      try {
        if (!toAdd.getIsDirectory()) {
          toAdd.setFileLocations(mMasterInfo.getFileLocations(toAdd.getId()));
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

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) {
    return;
  }

  /**
   * This function displays the first 5KB of a file if it is in ASCII format.
   * @param path The path of the file to display
   * @param request The HttpServletRequest object
   * @throws FileDoesNotExistException
   * @throws IOException
   * @throws InvalidPathException
   * @throws TException 
   */
  private void displayFile(String path, HttpServletRequest request) 
      throws FileDoesNotExistException, InvalidPathException, IOException {
    TachyonFS tachyonClient = TachyonFS.get(mMasterInfo.getMasterAddress());
    TachyonFile tFile = tachyonClient.getFile(path);
    if (tFile == null) {
      throw new FileDoesNotExistException(path);
    }

    InStream is = tFile.getInStream(OpType.READ_NO_CACHE);
    int len = Math.min(5 * Constants.KB, (int) tFile.length());
    byte[] data = new byte[len];
    is.read(data, 0, len);
    String fileData = CommonUtils.convertByteArrayToString(data);
    if (fileData == null) {
      fileData = "The requested file is not completely encoded in ascii";
    }
    is.close();

    try {
      tachyonClient.close();
    } catch (TException e) {
      LOG.error(e.getMessage());
    }
    request.setAttribute("fileData", fileData);
    return;
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
    pathInfos[0] = new UiFileInfo(mMasterInfo.getFileInfo(currentPath));
    for (int i = 1; i < splitPath.length - 1; i ++) {
      currentPath = currentPath + splitPath[i];
      pathInfos[i] = new UiFileInfo(mMasterInfo.getFileInfo(currentPath));
      currentPath = currentPath + Constants.PATH_SEPARATOR;
    }
    request.setAttribute("pathInfos", pathInfos);
    return;
  }
}