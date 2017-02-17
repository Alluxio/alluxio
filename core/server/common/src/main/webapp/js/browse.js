/**
 *
 rebuild the browse page navigator pill like breadcrumbs.
 *
 @param {string} path current navigator path
 *
 */
function rebuildNavPill(path) {
  if (path.length > 0 && path.charAt(path.length - 1) === '/') {
    path = path.substr(0, path.length - 1);
  }
  var pathSeg = path.split("/");
  if (path === "/")
    pathSeg.length = 1;
  if (pathSeg != null && pathSeg.length > 0) {
    pathSeg[0] = "/";
    $("#pathUl").children().remove();
    var curPath = "";
    $.each(pathSeg, function (index, value) {
      if (value.length === 0)
        return;
      curPath += value;

      var liNode = document.createElement("li");
      var aNode = document.createElement("a");
      $(aNode).attr("href", "javascript:void(0)");
      $(aNode).attr("onClick", 'generateData("' + curPath + '")');
      $(aNode).append(value);
      if (index === pathSeg.length - 1) {
        $(liNode).attr("class", "active");
      }
      liNode.appendChild(aNode);
      $("#pathUl").append(liNode);

      if (value !== "/") {
        curPath += "/"
      }
    })
  }
}

function generateData(path) {
  $.ajax({
    url: ajaxUrl,
    type: 'post',
    dataType: 'json',
    data: {
      path: path
    },
    success: function (json) {
      $(".text-error").html(json.argumentMap.invalidPathError);
      globalScope.gridOptions.data = json.pageData;
      globalScope.gridApi.grid.refresh();
      $("#pathInput").val(path);
      rebuildNavPill(path);
    },
    error: function () {
      alert("Path <" + path + "> was not found.");
    }
  });
}

function getInputPath() {
  var path = $.trim($("#pathInput").val());
  return path;
}

function changeDir() {
  var path = getInputPath();
  generateData(path);
}

function initGrid() {
  var app = angular.module(
      'app', ['ui.grid', 'ui.grid.pagination', 'ui.grid.resizeColumns', 'ui.grid.moveColumns']);
  app.controller(
      'MainCtrl',
      ['$scope', '$http', 'uiGridConstants', function ($scope, $http, uiGridConstants) {
        globalScope = $scope;
        function cellValueTooltip(row, col) {
          return $scope.gridApi.grid.getCellValue(row, col);
        }

        $scope.gridOptions = {
          enableFiltering: true,
          paginationPageSizes: [25, 50, 75, 100],
          paginationPageSize: 25,
          enableGridMenu: true,
          onRegisterApi: function (gridApi) {
            $scope.gridApi = gridApi;
            if (!showPermissions) {
              $scope.gridOptions.columnDefs[4].visible = false;
              $scope.gridOptions.columnDefs[5].visible = false;
              $scope.gridOptions.columnDefs[6].visible = false;
            }
            $scope.gridOptions.columnDefs[7].visible = false;

          },
          columnDefs: [
            {
              field: 'name', name: "File Name",
              cellTemplate: '<nobr>' +
              '<i ng-if="row.entity.isDirectory" class="icon-folder-close"></i>' +
              '<i ng-if="row.entity.isDirectory === false" class="icon-file"></i>' +
              '<a href="javascript:void(0)" ng-click=grid.appScope.goPath(row)>' +
              '{{row.entity.name}}</a>' +
              '</nobr>',
              cellTooltip: cellValueTooltip
            },
            {
              field: 'size', name: "Size", width: '10%',
              cellTooltip: cellValueTooltip
            },
            {
              field: 'blockSizeBytes', name: "Block Size", width: '8%',
              cellTooltip: cellValueTooltip
            },
            {
              field: 'inMemoryPercentage',
              name: "In-Memory",
              width: '6%',
              cellTemplate: '<div>' +
              '<i ng-if="row.entity.inMemoryPercentage === 0" class="icon-hdd icon-white"></i>' +
              '<i ng-if="row.entity.inMemoryPercentage !== 0" class="icon-hdd icon-black"></i>' +
              '{{row.entity.inMemoryPercentage}} %' +
              '</div>',
              cellTooltip: cellValueTooltip
            },
            {
              field: 'mode',
              name: "Mode", width: '8%',
              cellTooltip: cellValueTooltip,
            },
            {
              field: 'owner',
              name: "Owner", width: '6%',
              cellTooltip: cellValueTooltip
            },
            {
              field: 'group',
              name: "Group", width: '6%',
              cellTooltip: cellValueTooltip
            },
            {
              field: 'persistenceState',
              displayName: "Persistence State",
              width: '10%',
              cellTooltip: cellValueTooltip
            },
            {
              field: 'pinned',
              displayName: "Pin",
              width: '5%',
              filter: {
                type: uiGridConstants.filter.SELECT,
                selectOptions: [{value: true, label: 'YES'}, {value: false, label: 'NO'}]
              },
              cellFilter: 'pinned',
              cellTooltip: cellValueTooltip
            },
            {
              field: 'creationTime',
              displayName: "Creation Time",
              width: '10%',
              cellTooltip: cellValueTooltip
            },
            {
              field: 'modificationTime',
              displayName: "Modification Time",
              width: '10%',
              cellFilter: 'date:"longDate"',
              cellTooltip: cellValueTooltip
            }
          ],
        };

        $scope.toggleFiltering = function () {
          $scope.gridOptions.enableFiltering = !$scope.gridOptions.enableFiltering;
          $scope.gridApi.core.notifyDataChange(uiGridConstants.dataChange.COLUMN);
        };
        $scope.goPath = function (row) {
          if (!row.entity.isDirectory) {
            var path = encodeURI(row.entity.absolutePath);
            window.open("./" + base + "?path=" + path);
          } else {
            generateData(row.entity.absolutePath);
          }
        }
      }])
      .filter('pinned', function () {
        return function (input) {
          return input ? "YES" : "NO";
        };
      });
}

var ajaxUrl = '/browse/jumpPath.ajax';
var globalScope;
initGrid();

$(document).ready(function () {
  if (base === "./browse") {
    $("#pathInput").val(currentDir);
    $("#goBtn").click(changeDir);
    $("#pathInput").keydown(function (e) {
      if (e.keyCode === 13) {
        changeDir();
      }
    });
  } else {
    ajaxUrl = '/browse/browseLog.ajax';
    $("#pathNav").hide();
  }
  generateData("");
});

