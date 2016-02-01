<%@ page import="java.util.*" %>
<%@ page import="tachyon.web.*" %>

<script type="text/javascript">
  jsonString =  ${param.operationMetrics};
</script>

<div class="accordion span14" id="accordion4">
  <div class="accordion-group">
    <div class="accordion-heading">
      <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion4" href="#data4">
        <h4>Operations</h4>
      </a>
    </div>

    <div id="data4" class="accordion-body collapse in">
      <div class="accordion-inner">

        <table class="table table-condensed">
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
              <th>
                <div id="DirectoriesCreated">
                  <script type="text/javascript">
                    linearGraph("#DirectoriesCreated", jsonString["master.DirectoriesCreated"]);
                  </script>
                </div>

              </th>
              <th>
                <div id="FileBlockInfosGot">
                  <script type="text/javascript">
                    linearGraph("#FileBlockInfosGot", jsonString["master.FileBlockInfosGot"]);
                  </script>
                </div>
              </th>
            </tr>
            <tr>
              <th>File Infos Got</th>
              <th>Files Completed</th>
            </tr>
            <tr>
              <th>
                <div id="FileInfosGot">
                  <script type="text/javascript">
                    linearGraph("#FileInfosGot", jsonString["master.FileInfosGot"]);
                  </script>
                </div>

              </th>
              <th>
                <div id="FilesCompleted">
                  <script type="text/javascript">
                    linearGraph("#FilesCompleted", jsonString["master.FilesCompleted"]);
                  </script>
                </div>
              </th>
            </tr>
            <tr>
              <th>Files Created</th>
              <th>Files Freed</th>
            </tr>
            <tr>
              <th>
                <div id="FilesCreated">
                  <script type="text/javascript">
                    linearGraph("#FilesCreated", jsonString["master.FilesCreated"]);
                  </script>
                </div>
              </th>
              <th>
                <div id="FilesFreed">
                  <script type="text/javascript">
                    linearGraph("#FilesFreed", jsonString["master.FilesFreed"]);
                  </script>
                </div>
              </th>
            </tr>
            <tr>
              <th>Files Persisted</th>
              <th>Files Pinned</th>
            </tr>
            <tr>
              <th>
                <div id="FilesPersisted">
                  <script type="text/javascript">
                    linearGraph("#FilesPersisted", jsonString["master.FilesPersisted"]);
                  </script>
                </div>
              </th>
              <th>
                <div id="FilesPinned">
                  <script type="text/javascript">
                    linearGraph("#FilesPinned", jsonString["master.FilesPinned"]);
                  </script>
                </div>
              </th>
            </tr>
            <tr>
              <th>New Blocks Got</th>
              <th>Paths Deleted</th>
            </tr>
            <tr>
              <th>
                <div id="NewBlocksGot">
                  <script type="text/javascript">
                    linearGraph("#NewBlocksGot", jsonString["master.NewBlocksGot"]);
                  </script>
                </div>
              </th>
              <th>
                <div id="PathsDeleted">
                  <script type="text/javascript">
                    linearGraph("#PathsDeleted", jsonString["master.PathsDeleted"]);
                  </script>
                </div>
              </th>
            </tr>

            <tr>
              <th>Paths Mounted</th>
              <th>Paths Renamed</th>
            </tr>
            <tr>
              <th>
                <div id="PathsMounted">
                  <script type="text/javascript">
                    linearGraph("#PathsMounted", jsonString["master.PathsMounted"]);
                  </script>
                </div>
              </th>
              <th>
                <div id="PathsRenamed">
                  <script type="text/javascript">
                    linearGraph("#PathsRenamed", jsonString["master.PathsRenamed"]);
                  </script>
                </div>
              </th>
            </tr>
            <tr>
              <th>PathsUnmounted</th>
              <th></th>
            </tr>
            <tr>
              <th>
                <div id="PathsUnmounted">
                  <script type="text/javascript">
                    linearGraph("#PathsUnmounted", jsonString["master.PathsUnmounted"]);
                  </script>
                </div>
              </th>
              <th></th>
            </tr>
          </tbody>
        </table>

      </div>
    </div>
  </div>
</div>