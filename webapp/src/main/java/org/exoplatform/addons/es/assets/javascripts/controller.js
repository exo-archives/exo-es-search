/**
 * Created by TClement on 10/5/15.
 */
'use strict';

var indexingManagementController = angular.module('indexingManagementController', []);

indexingManagementController.controller('ConnectorListCtrl', ['$scope', 'Connector',
    function($scope, Connector) {

        Connector.getConnectors().success(function(data) {
            $scope.connectors = data;
        });

        $scope.reindexConnector = function(connectorType) {
            Connector.reindexConnector(connectorType).then(function(response) {
                console.log("Reindex request response = "+response)
            });
        };

        $scope.disableIndexConnector = function(connectorType) {
            Connector.disableIndexConnector(connectorType).then(function(response) {
                console.log("Disable request response = "+response)
            });
        };

        $scope.enableIndexConnector = function(connectorType) {
            Connector.enableIndexConnector(connectorType).then(function(response) {
                console.log("Disable request response = "+response)
            });
        };

    }]);