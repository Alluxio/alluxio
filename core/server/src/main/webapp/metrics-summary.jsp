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
        <table class="table">
          <thead>
            <th></th>
            <th></th>
          </thead>
          <tbody>
            <tr>
              <th>Directories Created</th>
              <th><%= request.getAttribute("master.DirectoriesCreated") %></th>
              <th>File Block Infos Got</th>
              <th><%= request.getAttribute("master.FileBlockInfosGot") %></th>
            </tr>
            <tr>
              <th>File Infos Got</th>              
              <th><%= request.getAttribute("master.FileInfosGot") %></th>
              <th>Files Completed</th>
              <th><%= request.getAttribute("master.FilesCompleted") %></th>
            </tr>
            <tr>
              <th>Files Created</th>
              <th><%= request.getAttribute("master.FilesCreated") %></th>
              <th>Files Freed</th>
              <th><%= request.getAttribute("master.FilesFreed") %> </th>
            </tr>
            <tr>
              <th>Files Persisted</th>
              <th><%= request.getAttribute("master.FilesPersisted") %></th>
              <th>Files Pinned</th>
              <th><%= request.getAttribute("master.FilesPinned") %></th>              
            </tr>
            <tr>
              <th>New Blocks Got</th>
              <th><%= request.getAttribute("master.NewBlocksGot") %></th>
              <th>Paths Deleted</th>
              <th><%= request.getAttribute("master.PathsDeleted") %></th>              
            </tr>
            <tr>
              <th>Paths Mounted</th>
              <th><%= request.getAttribute("master.PathsMounted") %></th>
              <th>Paths Renamed</th>
              <th><%= request.getAttribute("master.PathsRenamed") %></th>
            </tr>
            <tr>
              <th>PathsUnmounted</th>
              <th><%= request.getAttribute("master.PathsUnmounted") %></th>
              <th></th>
            </tr>
          </tbody>
        </table>

      </div>
    </div>
  </div>
</div>