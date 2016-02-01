<%@ page import="java.util.*" %>
<%@ page import="tachyon.web.*" %>

<div class="accordion span14" id="accordion4">
  <div class="accordion-group">
    <div class="accordion-heading">
      <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion4" href="#data4">
        <h4>Operations</h4>
      </a>
    </div>    

    <div id="data4" class="accordion-body collapse in">
      <div class="accordion-inner">

        <table class="table table-condensed table-centered">
          <thead>
            <th></th>
            <th></th>
          </thead>
          <tbody>
            <tr>
              <th>Directories Created</th>
              <th>File Block Infos Got</th>
              <th>File Infos Got</th>
            </tr>
            <tr>
              <td>
                <div id="DirectoriesCreated" class="huge">                  
                    <%= request.getAttribute("master.DirectoriesCreated") %>
                </div>
              </td>
              <td>
                <div id="FileBlockInfosGot" class="huge">
                    <%= request.getAttribute("master.FileBlockInfosGot") %>
                </div>
              </td>
              <td>
                <div id="FileInfosGot" class="huge">
                    <%= request.getAttribute("master.FileInfosGot") %>
                </div>
              </td>
            </tr>
            <tr>
              <th>Files Completed</th>
              <th>Files Created</th>
              <th>Files Freed</th>
            </tr>
            <tr>
              <td>
                <div id="FilesCompleted" class="huge">
                    <%= request.getAttribute("master.FilesCompleted") %>
                </div>
              </td>
              <td>
                <div id="FilesCreated" class="huge">
                    <%= request.getAttribute("master.FilesCreated") %>
                </div>
              </td>
              <td>
                <div id="FilesFreed" class="huge">
                    <%= request.getAttribute("master.FilesFreed") %> 
                </div>
              </td>
            </tr>
            <tr>
              <th>Files Persisted</th>
              <th>Files Pinned</th>
              <th>New Blocks Got</th>
            </tr>
            <tr>
              <td>
                <div id="FilesPersisted" class="huge">
                    <%= request.getAttribute("master.FilesPersisted") %>
                </div>
              </td>
              <td>
                <div id="FilesPinned" class="huge">
                    <%= request.getAttribute("master.FilesPinned") %>
                </div>
              </td>
              <td>
                <div id="NewBlocksGot" class="huge">
                    <%= request.getAttribute("master.NewBlocksGot") %>
                </div>
              </td>
            </tr>
            <tr>
              <th>Paths Deleted</th>
              <th>Paths Mounted</th>
              <th>Paths Renamed</th>
            </tr>
            <tr>
              <td>
                <div id="PathsDeleted" class="huge">
                    <%= request.getAttribute("master.PathsDeleted") %>
                </div>
              </td>
              <td>
                <div id="PathsMounted" class="huge">
                    <%= request.getAttribute("master.PathsMounted") %>
                </div>
              </td>
              <td>
                <div id="PathsRenamed" class="huge">
                    <%= request.getAttribute("master.PathsRenamed") %>
                </div>
              </td>
            </tr>
            <tr>
              <th>PathsUnmounted</th>
              <th></th>
              <th></th>
            </tr>
            <tr>
              <td>
                <div id="PathsUnmounted" class="huge">
                    <%= request.getAttribute("master.PathsUnmounted") %>
                </div>
              </td>
              <td></td>
              <td></td>
            </tr>
          </tbody>
        </table>

      </div>
    </div>
  </div>
</div>