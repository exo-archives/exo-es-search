/**
 * Created by TClement on 10/7/15.
 */

'use strict';

var indexingManagementServices = angular.module('indexingManagementServices', []);

indexingManagementServices.service('Connector', ['$http', function ($http) {

    var urlBase = '/rest/indexingManagement';

    this.getConnectors = function () {
        return $http.get(urlBase + '/connectors');
    };

    this.reindexConnector = function(connectorType) {
        return $http.get(urlBase + '/connector/reindex/' + connectorType);
    };

    this.disableIndexConnector = function(connectorType) {
        return $http.get(urlBase + '/connector/disable/' + connectorType);
    };

    this.enableIndexConnector = function(connectorType) {
        return $http.get(urlBase + '/connector/enable/' + connectorType);
    };

}]);