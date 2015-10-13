/**
 * Created by TClement on 10/7/15.
 */

'use strict';

var indexingManagementServices = angular.module('indexingManagementServices', []);
var urlBase = '/rest/indexingManagement';

indexingManagementServices.service('Stat', ['$http', function ($http) {

    this.getNbConnectors = function () {
        return $http.get(urlBase + '/connector/_count');
    };

    this.getNbIndexingOperation = function() {
        return $http.get(urlBase + '/operation/_count');
    };

    this.getNbIndexingError = function() {
        return $http.get(urlBase + '/error/_count');
    };

}]);

indexingManagementServices.service('Connector', ['$http', function ($http) {

    var urlConnectorBase = urlBase + '/connector';

    this.getConnectors = function () {
        return $http.get(urlConnectorBase);
    };

    this.reindexConnector = function(connectorType) {
        return $http.get(urlConnectorBase + '/' + connectorType + '/_reindex');
    };

    this.disableIndexConnector = function(connectorType) {
        return $http.get(urlConnectorBase + '/' + connectorType + '/_disable');
    };

    this.enableIndexConnector = function(connectorType) {
        return $http.get(urlConnectorBase + '/' + connectorType + '/_enable');
    };

}]);

indexingManagementServices.service('Error', ['$http', function ($http) {

    var urlErrorBase = urlBase + '/error';

    this.getErrors = function () {
        return $http.get(urlErrorBase);
    };

    this.addToQueue = function(id) {
        return $http.post(urlErrorBase + '/' + id + '/_addToQueue');
    };

}]);

indexingManagementServices.service('Operation', ['$http', function ($http) {

    var urlOperationBase = urlBase + '/operation';

    this.getOperations = function () {
        return $http.get(urlOperationBase);
    };

    this.deleteOperation = function(id) {
        return $http.delete(urlOperationBase + '/' + id);
    };

}]);