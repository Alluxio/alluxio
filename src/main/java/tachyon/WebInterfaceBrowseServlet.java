package tachyon;

import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;


import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;


public class WebInterfaceBrowseServlet extends HttpServlet {
  MasterInfo mMasterInfo;

  public WebInterfaceBrowseServlet(MasterInfo MI) {
    this.mMasterInfo = MI;
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
      filesInfo = this.mMasterInfo.getFilesInfo(currentPath);
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
      request.setAttribute("currentDirectory", new FileInfo(this.mMasterInfo.getFileInfo("/")));
    }
    String currentPath = new String("");
    FileInfo[] pathInfos = new FileInfo[splitPath.length];
    for (int i = 0; i < splitPath.length; i++) {
      currentPath = currentPath + splitPath[i] + "/";
      if (i == splitPath.length - 1) {
        request.setAttribute("currentDirectory", 
                            new FileInfo(this.mMasterInfo.getFileInfo(currentPath)));
      } else {
        pathInfos[i] = new FileInfo(this.mMasterInfo.getFileInfo(currentPath));
      }
    }
    request.setAttribute("pathInfos", pathInfos);
  }

  // Class to make referencing file objects more intuitive. Mainly to avoid implicit association
  // by array indexes.

  public class FileInfo {
    private String name;
    private String absolutePath;
    private long size;
    private boolean inMemory;
    private boolean isDirectory;

    private FileInfo(ClientFileInfo fileInfo) {
      this.name = fileInfo.getName();
      this.absolutePath = fileInfo.getPath();
      this.size = fileInfo.getSizeBytes();
      this.inMemory = fileInfo.inMemory;
      this.isDirectory = fileInfo.isFolder;
    }

    public String getName() {
      if (this.isDirectory) {
        return this.name + "/";
      } else {
        return this.name;
      }
    }

    public String getAbsolutePath() {
      return this.absolutePath;
    }

    public long getSize() {
      return this.size;
    }

    public boolean getInMemory() {
      return this.inMemory;
    }

    public boolean getIsDirectory() {
      return this.isDirectory;
    }

  }
}