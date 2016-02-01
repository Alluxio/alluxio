<%@ page import="java.util.*" %>
<%@ page import="tachyon.web.*" %>

<div class="row-fluid">
  <div class="accordion span14" id="accordion4">
    <div class="accordion-group">
      <div class="accordion-heading">
        <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion4" href="#data4">
          <h4>Operations</h4>
        </a>
      </div>

      <script type="text/javascript">
        jsonString = $ {param.operationMetrics};
      </script>

      <div id="data4" class="accordion-body collapse in">
        <div class="accordion-inner">

          <table class="table table-condensed table-header-centered">
            <thead>
              <th></th>
              <th></th>
            </thead>
            <tbody>
              <tr>
                <th>Directories Created</th>
                <th>File Block Infos Got</th>
              </tr>
              <tr>
                <td>
                  <div id="DirectoriesCreated">
                    <script type="text/javascript">
                      linearGraph("#DirectoriesCreated", jsonString["master.DirectoriesCreated"]);
                    </script>
                  </div>

                </td>
                <td>
                  <div id="FileBlockInfosGot">
                    <script type="text/javascript">
                      linearGraph("#FileBlockInfosGot", jsonString["master.FileBlockInfosGot"]);
                    </script>
                  </div>
                </td>
              </tr>
              <tr>
                <th>File Infos Got</th>
                <th>Files Completed</th>
              </tr>
              <tr>
                <td>
                  <div id="FileInfosGot">
                    <script type="text/javascript">
                      linearGraph("#FileInfosGot", jsonString["master.FileInfosGot"]);
                    </script>
                  </div>
                </td>
                <td>
                  <div id="FilesCompleted">
                    <script type="text/javascript">
                      linearGraph("#FilesCompleted", jsonString["master.FilesCompleted"]);
                    </script>
                  </div>
                </td>
              </tr>
              <tr>
                <th>Files Created</th>
                <th>Files Freed</th>
              </tr>
              <tr>
                <td>
                  <div id="FilesCreated">
                    <script type="text/javascript">
                      linearGraph("#FilesCreated", jsonString["master.FilesCreated"]);
                    </script>
                  </div>
                </td>
                <td>
                  <div id="FilesFreed">
                    <script type="text/javascript">
                      linearGraph("#FilesFreed", jsonString["master.FilesFreed"]);
                    </script>
                  </div>
                </td>
              </tr>
              <tr>
                <th>Files Persisted</th>
                <th>Files Pinned</th>
              </tr>
              <tr>
                <td>
                  <div id="FilesPersisted">
                    <script type="text/javascript">
                      linearGraph("#FilesPersisted", jsonString["master.FilesPersisted"]);
                    </script>
                  </div>
                </td>
                <td>
                  <div id="FilesPinned">
                    <script type="text/javascript">
                      linearGraph("#FilesPinned", jsonString["master.FilesPinned"]);
                    </script>
                  </div>
                </td>
              </tr>
              <tr>
                <th>New Blocks Got</th>
                <th>Paths Deleted</th>
              </tr>
              <tr>
                <td>
                  <div id="NewBlocksGot">
                    <script type="text/javascript">
                      linearGraph("#NewBlocksGot", jsonString["master.NewBlocksGot"]);
                    </script>
                  </div>
                </td>
                <td>
                  <div id="PathsDeleted">
                    <script type="text/javascript">
                      linearGraph("#PathsDeleted", jsonString["master.PathsDeleted"]);
                    </script>
                  </div>
                </td>
              </tr>
              <tr>
                <th>Paths Mounted</th>
                <th>Paths Renamed</th>
              </tr>
              <tr>
                <td>
                  <div id="PathsMounted">
                    <script type="text/javascript">
                      linearGraph("#PathsMounted", jsonString["master.PathsMounted"]);
                    </script>
                  </div>
                </td>
                <td>
                  <div id="PathsRenamed">
                    <script type="text/javascript">
                      linearGraph("#PathsRenamed", jsonString["master.PathsRenamed"]);
                    </script>
                  </div>
                </td>
              </tr>
              <tr>
                <th>PathsUnmounted</th>
                <th></th>
              </tr>
              <tr>
                <td>
                  <div id="PathsUnmounted">
                    <script type="text/javascript">
                      linearGraph("#PathsUnmounted", jsonString["master.PathsUnmounted"]);
                    </script>
                  </div>
                </td>
                <td></td>
              </tr>
            </tbody>
          </table>

        </div>
      </div>
    </div>
  </div>
</div>