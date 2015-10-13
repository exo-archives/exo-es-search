/**
 * Created by TClement on 10/5/15.
 */
'use strict';

var indexingManagementController = angular.module('indexingManagementController', []);

indexingManagementController.controller('IndexingStatCtrl', ['$scope', '$interval', 'Stat',
    function($scope, $interval, Stat) {

        //Loads and populates the stats
        this.loadStats = function (){

            Stat.getNbConnectors().success(function(data) {
                $scope.nbConnectors = data;
            });

            Stat.getNbIndexingOperation().success(function(data) {
                $scope.nbOperations = data;
            });

            Stat.getNbIndexingError().success(function(data) {
                $scope.nbErrors = data;
            });

        }
        //Put in interval, first trigger after 5 seconds
        $interval(function(){
            this.loadStats();
        }.bind(this), 5000);

        //invoke initialy
        this.loadStats();

    }
]);

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

    }
]);

indexingManagementController.controller('ErrorListCtrl', ['$scope', 'Error',
    function($scope, Error) {

        Error.getErrors().success(function(data) {
            $scope.errors = data;
        });

        $scope.addToQueue = function(id) {
            Error.addToQueue(id).then(function(response) {
                console.log("AddToQueue request response = "+response)
            });
        };

    }
]);

indexingManagementController.controller('OperationListCtrl', ['$scope', 'Operation',
    function($scope, Operation) {

        Operation.getOperations().success(function(data) {
            $scope.operations = data;
        });

        $scope.deleteOperation = function(id) {
            Operation.deleteOperation(id).then(function(response) {
                console.log("deleteOperation request response = "+response)
            });
        };

    }
]);
