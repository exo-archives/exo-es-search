/**
 * Created by TClement on 12/15/15.
 */

define('operationController', ['SHARED/jquery', 'indexingManagementApi'],
    function($, indexingManagementApi) {

        var operationController = function operationController() {
            var self = this;

            //Get the Indexing management REST Service API
            var myIndexingManagementApi = new indexingManagementApi();

            self.init = function() {
                console.log('init operationController');
            }

        }

        return operationController;
    }
);
