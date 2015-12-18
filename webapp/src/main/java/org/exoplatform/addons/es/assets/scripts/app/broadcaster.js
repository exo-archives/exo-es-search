/**
 * Created by TClement on 12/17/15.
 */

define('appBroadcaster', ['SHARED/jquery', 'operationController', 'statController', 'connectorController'],
    function($, operationController, statController, connectorController) {


        //Controller
        var myStatController = new statController();
        var myOperationController = new operationController();
        var myConnectorController = new connectorController();

        var appBroadcaster = function appBroadcaster() {
            var self = this;

            self.onReindexConnector = function() {
                myConnectorController.refreshConnectorList();
                myStatController.refreshStatNbOperation();
                myOperationController.refereshOperationList();
            }

            self.onDeleteOperation = function() {
                myOperationController.refereshOperationList();
                myStatController.refreshStatNbOperation();
            }

        }

        return appBroadcaster;
    }
);