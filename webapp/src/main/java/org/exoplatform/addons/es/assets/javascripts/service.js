/**
 * Created by TClement on 10/7/15.
 */

'use strict';

var indexingManagementServices = angular.module('indexingManagementServices', []);

indexingManagementServices.service('Connector', ['$http', function ($http) {

    var urlBase = '/rest/indexingManagement';

    this.getConnectors = function () {
        return $http.get(urlBase + '/connector');
    };

    this.reindexConnector = function(connectorType) {
        return $http.get(urlBase + '/connector/' + connectorType + '/_reindex');
    };

    this.disableIndexConnector = function(connectorType) {
        return $http.get(urlBase + '/connector/' + connectorType + '/_disable');
    };

    this.enableIndexConnector = function(connectorType) {
        return $http.get(urlBase + '/connector/' + connectorType + '/_enable');
    };

}]);