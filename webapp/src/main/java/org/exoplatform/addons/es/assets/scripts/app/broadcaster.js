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
                myConnectorController.updateConnectorList();
                myStatController.updateStatNbOperation();
                myOperationController.updateOperationList();
            }

            self.onDeleteOperation = function() {
                myOperationController.updateOperationList();
                myStatController.updateStatNbOperation();
            }

        }

        return appBroadcaster;
    }
);