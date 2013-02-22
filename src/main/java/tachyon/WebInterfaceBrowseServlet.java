package tachyon;
import java.util.List;
import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;

public class WebInterfaceBrowseServlet extends HttpServlet {
  MasterInfo mMasterInfo;

  public WebInterfaceBrowseServlet(MasterInfo MI) {
    mMasterInfo = MI;
  }

  protected void doGet(HttpServletRequest request, HttpServletResponse response) 
      throws ServletException, IOException {
    request.setAttribute("invalidPathError", "");
    List<ClientFileInfo> filesInfo = null;
    String currentPath = request.getParameter("path");

    if (currentPath.isEmpty()) {
      currentPath = "/";
    }
    request.setAttribute("currentPath", currentPath);

    try {
      this.setPathDirectories(currentPath, request);
      filesInfo = mMasterInfo.getFilesInfo(currentPath);
    } catch (FileDoesNotExistException fdne) {
      request.setAttribute("invalidPathError", "Error: not a valid directory");
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
      return; // For clarity
    } catch (InvalidPathException ipe) {
      request.setAttribute("invalidPathError", "Error: not a valid directory");
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
      return; // For clarity
    }

    int index = 0;
    FileInfo[] fileInfos = new FileInfo[filesInfo.size()];
    for (ClientFileInfo fileInfo : filesInfo) {
      fileInfos[index++] = new FileInfo(fileInfo);
    }
    request.setAttribute("fileInfos", fileInfos);

    getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
  }



  protected void doPost(HttpServletRequest request, HttpServletResponse response) {
    return;
  }

  // This function sets the fileinfos for folders that are in the path to the current directory
  private void setPathDirectories(String path, HttpServletRequest request) 
      throws FileDoesNotExistException, InvalidPathException {
    String[] splitPath = path.split("/");
    if (splitPath.length == 0) {
      request.setAttribute("currentDirectory", new FileInfo(mMasterInfo.getFileInfo("/")));
    }
    String currentPath = new String("");
    FileInfo[] pathInfos = new FileInfo[splitPath.length];
    for (int i = 0; i < splitPath.length; i++) {
      currentPath = currentPath + splitPath[i] + "/";
      if (i == splitPath.length - 1) {
        request.setAttribute("currentDirectory", 
                            new FileInfo(mMasterInfo.getFileInfo(currentPath)));
      } else {
        pathInfos[i] = new FileInfo(mMasterInfo.getFileInfo(currentPath));
      }
    }
    request.setAttribute("pathInfos", pathInfos);
  }

  // Class to make referencing file objects more intuitive. Mainly to avoid implicit association
  // by array indexes.
  public class FileInfo {
    private String mName;
    private String mAbsolutePath;
    private long mSize;
    private boolean mInMemory;
    private boolean mIsDirectory;

    private FileInfo(ClientFileInfo fileInfo) {
      mName = fileInfo.getName();
      mAbsolutePath = fileInfo.getPath();
      mSize = fileInfo.getSizeBytes();
      mInMemory = fileInfo.inMemory;
      mIsDirectory = fileInfo.isFolder;
    }

    public String getName() {
      if (mIsDirectory) {
        return mName + "/";
      } else {
        return mName;
      }
    }

    public String getAbsolutePath() {
      return mAbsolutePath;
    }

    public long getSize() {
      return mSize;
    }

    public boolean getInMemory() {
      return mInMemory;
    }

    public boolean getIsDirectory() {
      return mIsDirectory;
    }
  }
}