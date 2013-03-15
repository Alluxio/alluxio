package tachyon;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;

public class WebInterfaceBrowseServlet extends HttpServlet {
  private static final long serialVersionUID = 6121623049981468871L;

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

    private UiFileInfo(ClientFileInfo fileInfo) {
      ID = fileInfo.getId();
      NAME = fileInfo.getName();
      ABSOLUATE_PATH = fileInfo.getPath();
      CHECKPOINT_PATH = fileInfo.getCheckpointPath();
      SIZE = fileInfo.getSizeBytes();
      CREATION_TIME_MS = fileInfo.getCreationTimeMs();
      IN_MEMORY = fileInfo.isInMemory();
      IS_DIRECTORY = fileInfo.isFolder();
    }

    public int getId() {
      return ID;
    }

    public String getName() {
      if (ABSOLUATE_PATH.equals(Config.SEPARATOR)) {
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

    @Override
    public int compareTo(UiFileInfo o) {
      return ABSOLUATE_PATH.compareTo(o.getAbsolutePath());
    }
  }

  public WebInterfaceBrowseServlet(MasterInfo masterInfo) {
    mMasterInfo = masterInfo;
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) 
      throws ServletException, IOException {
    request.setAttribute("debug", Config.DEBUG);

    request.setAttribute("masterNodeAddress", mMasterInfo.getMasterAddress().toString());
    request.setAttribute("invalidPathError", "");
    List<ClientFileInfo> filesInfo = null;
    String currentPath = request.getParameter("path");

    if (currentPath.isEmpty()) {
      currentPath = "/";
    }
    request.setAttribute("currentPath", currentPath);

    try {
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
      fileInfos.add(new UiFileInfo(fileInfo));
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
   * This function sets the fileinfos for folders that are in the path to the current directory.
   * 
   * @param path
   * @param request
   * @throws FileDoesNotExistException
   * @throws InvalidPathException
   */
  private void setPathDirectories(String path, HttpServletRequest request) 
      throws FileDoesNotExistException, InvalidPathException {
    request.setAttribute("currentDirectory", new UiFileInfo(mMasterInfo.getFileInfo(path)));
    if (path.equals(Config.SEPARATOR)) {
      request.setAttribute("pathInfos", new UiFileInfo[0]);
      return;
    }

    String[] splitPath = path.split(Config.SEPARATOR);
    UiFileInfo[] pathInfos = new UiFileInfo[splitPath.length - 1];
    String currentPath = Config.SEPARATOR;
    pathInfos[0] = new UiFileInfo(mMasterInfo.getFileInfo(currentPath));
    for (int i = 1; i < splitPath.length - 1; i ++) {
      currentPath = currentPath + splitPath[i];
      pathInfos[i] = new UiFileInfo(mMasterInfo.getFileInfo(currentPath));
      currentPath = currentPath + Config.SEPARATOR;
    }
    request.setAttribute("pathInfos", pathInfos);
    return;
  }
}