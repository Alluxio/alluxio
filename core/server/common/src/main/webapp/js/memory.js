function generateData($scope) {
  $.ajax({
    url: '/memory/getFiles.ajax',
    type: 'post',
    dataType: 'json',
    data: {
      paginationOptions: JSON.stringify(paginationOptions)
    },
    success: function (json) {
      $(".text-error").html(json.argumentMap.fatalError);
      $scope.gridOptions.data = json.pageData;
      $scope.gridOptions.totalItems = json.totalCount;
      $scope.gridApi.grid.refresh();
    },
    error: function () {
      alert("something wrong.");
    }
  });
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
          useExternalPagination: true,
          useExternalSorting: true,
          useExternalFiltering: true,
          enableFiltering: true,
          enableGridMenu: true,
          paginationPageSizes: [25, 50, 75, 100],
          paginationPageSize: 25,
          onRegisterApi: function (gridApi) {
            $scope.gridApi = gridApi;

            if (!showPermissions) {
              $scope.gridOptions.columnDefs[3].visible = false;
              $scope.gridOptions.columnDefs[4].visible = false;
              $scope.gridOptions.columnDefs[5].visible = false;
            }

            $scope.gridApi.core.on.sortChanged($scope, function (grid, sortColumns) {
              paginationOptions.sorters = []
              $.each(sortColumns, function (index, value) {
                paginationOptions.sorters.push({
                  "field": value.field,
                  "direction": value.sort.direction
                });
              });
              generateData($scope);
            });
            gridApi.pagination.on.paginationChanged($scope, function (newPage, pageSize) {
              paginationOptions.pageNumber = newPage;
              paginationOptions.pageSize = pageSize;
              generateData($scope);
            });
            $scope.gridApi.core.on.filterChanged($scope, function () {
              var grid = this.grid;
              console.log("filterChanged!:");
              paginationOptions.filters = [];
              $.each(grid.columns, function (index, value) {
                if (value.filters[0].term != undefined && value.filters[0].term != "") {
                  paginationOptions.filters.push(
                      {"field": value.field, "term": value.filters[0].term});
                  console.log(value.field + "'s filter: " + value.filters[0].term);
                }
              });
              generateData($scope);
            });
          },

          columnDefs: [
            {
              field: 'absolutePath', name: "AbsolutePath",
              cellTemplate: '<nobr>' +
              '<i class="icon-file"></i>' +
              '<a href="javascript:void(0)" ng-click=grid.appScope.openFile(row)>' +
              '{{row.entity.absolutePath}}</a>' +
              '</nobr>',
              cellTooltip: cellValueTooltip
            },
            {
              field: 'size', name: "Size",
              width: '8%',
              cellTooltip: cellValueTooltip
              ,
            },
            {
              field: 'blockSizeBytes', name: "Block Size",
              width: '8%',
              cellTooltip: cellValueTooltip
            },

            {
              field: 'mode',
              name: "Mode",
              width: '8%',
              cellTooltip: cellValueTooltip
            },
            {
              field: 'owner',
              name: "Owner",
              width: '6%',
              cellTooltip: cellValueTooltip
            },
            {
              field: 'group',
              name: "Group",
              width: '6%',
              cellTooltip: cellValueTooltip
            },

            {
              field: 'pinned',
              displayName: "Pin",
              filter: {
                type: uiGridConstants.filter.SELECT,
                selectOptions: [{value: true, label: 'YES'}, {value: false, label: 'NO'}]
              },
              width: '4%',
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
              cellTooltip: cellValueTooltip
            }
          ],

        }
        $scope.toggleFiltering = function () {
          $scope.gridOptions.enableFiltering = !$scope.gridOptions.enableFiltering;
          $scope.gridApi.core.notifyDataChange(uiGridConstants.dataChange.COLUMN);
        };
        $scope.openFile = function (row) {
          var path = encodeURI(row.entity.absolutePath);
          window.open("./" + "browse" + "?path=" + path);
        }

      }])
      .filter('pinned', function () {
        return function (input) {
          return input ? "YES" : "NO";
        };
      });
}

var globalScope;
var paginationOptions = {
  pageNumber: 1,
  pageSize: 25,
  sorters: [],
  filters: []
};
initGrid();

$(document).ready(function () {
  paginationOptions.pageSize = globalScope.gridOptions.paginationPageSize;
  generateData(globalScope);
});
