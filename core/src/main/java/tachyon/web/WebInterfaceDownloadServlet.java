package tachyon.web;

import java.io.IOException;

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
import tachyon.master.MasterInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;

/**
 * Servlet for downloading a file
 */
public class WebInterfaceDownloadServlet extends HttpServlet {
  private static final long serialVersionUID = 7329267100965731815L;

  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private MasterInfo mMasterInfo;

  public WebInterfaceDownloadServlet(MasterInfo masterInfo) {
    mMasterInfo = masterInfo;
  }

  /**
   * Prepares for downloading a file
   * 
   * @param request The HttpServletRequest object
   * @param response The HttpServletReponse object
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    String currentPath = request.getParameter("path");
    if (currentPath == null || currentPath.isEmpty()) {
      currentPath = TachyonURI.SEPARATOR;
    }
    try {
      ClientFileInfo clientFileInfo = mMasterInfo.getClientFileInfo(currentPath);
      downloadFile(clientFileInfo.getPath(), request, response);
    } catch (FileDoesNotExistException fdne) {
      request.setAttribute("invalidPathError", "Error: Invalid Path " + fdne.getMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
    } catch (InvalidPathException ipe) {
      request.setAttribute("invalidPathError", "Error: Invalid Path " + ipe.getLocalizedMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
    }
  }

  /**
   * This function prepares for downloading a file.
   * 
   * @param path The path of the file to download
   * @param request The HttpServletRequest object
   * @param response The HttpServletResponse object
   * @throws FileDoesNotExistException
   * @throws IOException
   */
  private void downloadFile(String path, HttpServletRequest request, HttpServletResponse response)
      throws FileDoesNotExistException, IOException {
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
