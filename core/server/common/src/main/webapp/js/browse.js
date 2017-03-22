function initGrid() {
  var app = angular.module(
      'app', ['ui.grid', 'ui.grid.pagination', 'ui.grid.resizeColumns', 'ui.grid.moveColumns']);
  app.directive('nav', function () {
    var directive = {};
    directive.restrict = 'E';
    directive.replace = true;
    directive.template = '<li><a href="javascript:void(0)" ng-click="navPillClick(this)" '
        + 'path="{{path}}">{{name}}</a></li>',
        directive.compile = function (element, attributes) {
          var linkFunction = function ($scope, element, attributes) {
            element.html('<a href="javascript:void(0)" ng-click="navPillClick($event)" path="'
                + attributes.path + '">' + attributes.name + '</a>');
          }
          return linkFunction;
        };
    return directive;
  });
  app.controller(
      'MainCtrl', ['$scope', '$http', 'uiGridConstants', '$compile',
        function ($scope, $http, uiGridConstants, $compile) {
          var ajaxUrl = '/browse/jumpPath.ajax';

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
              $scope.gridOptions.columnDefs[7].visible = false;
              if (base === "./browse") {
                $("#pathInput").val("/");
                $("#pathInput").keydown(function (e) {
                  if (e.keyCode === 13) {
                    $scope.changeDir();
                  }
                });
                $scope.generateData("/");
              } else {
                ajaxUrl = '/browse/browseLog.ajax';
                $("#pathNav").hide();
                $scope.generateData("");
              }
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
              $scope.generateData(row.entity.absolutePath);
            }
          };
          $scope.changeDir = function () {
            var path = $scope.getInputPath();
            $scope.generateData(path);
          };
          /**
           *
           * rebuild the browse page navigator pill like breadcrumbs.
           *
           * @param {string} path current navigator path
           *
           */
          $scope.rebuildNavPill = function (path) {
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
                var navNode = document.createElement("nav");
                $(navNode).attr("path", curPath);
                $(navNode).attr("name", value);
                if (index === pathSeg.length - 1) {
                  $(navNode).attr("class", "active");
                }
                $("#pathUl").append(navNode);
                $compile($('#pathUl'))($scope);
                if (value !== "/") {
                  curPath += "/"
                }
              })
            }
          };
          $scope.navPillClick = function (e) {
            $scope.generateData($(e.target).attr("path"))
          };
          $scope.generateData = function (path) {
            $.ajax({
              url: ajaxUrl,
              type: 'post',
              dataType: 'json',
              data: {
                path: path
              },
              success: function (json) {
                $(".text-error").html(json.argumentMap.invalidPathError);
                if (!json.argumentMap.showPermissions) {
                  $scope.gridOptions.columnDefs[4].visible = false;
                  $scope.gridOptions.columnDefs[5].visible = false;
                  $scope.gridOptions.columnDefs[6].visible = false;
                }
                $scope.gridOptions.data = json.pageData;
                $scope.gridApi.grid.refresh();
                $("#pathInput").val(path);
                $scope.rebuildNavPill(path);
              },
              error: function () {
                alert("Path <" + path + "> was not found.");
              }
            });
          };
          $scope.getInputPath = function () {
            var path = $.trim($("#pathInput").val());
            return path;
          };
        }])
      .filter('pinned', function () {
        return function (input) {
          return input ? "YES" : "NO";
        };
      });
}

initGrid();
