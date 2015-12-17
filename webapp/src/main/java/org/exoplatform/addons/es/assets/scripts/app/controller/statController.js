/**
 * Created by TClement on 12/15/15.
 */

define('statController', ['SHARED/jquery', 'indexingManagementApi', 'appBroadcaster'],
    function($, indexingManagementApi, appBroadcaster) {

        //Service
        var myIndexingManagementApi = new indexingManagementApi();
        var myAppBroadcaster = new appBroadcaster();

        var statController = function statController() {
            var self = this;

            self.init = function() {

                //Init stats value
                self.updateStatNbConnector();
                self.updateStatNbOperation();
                self.updateStatNbError();

                //Set refresh interval for operations stats to 5 seconds
                setInterval(function(){
                    self.updateStatNbOperation();
                }, 5000);

            }

            self.updateStatNbConnector = function() {
                updateStatNbConnectorValue()
            }

            self.updateStatNbOperation = function() {
                updateStatNbOperationValue()
            }

            self.updateStatNbError = function() {
                updateStatNbErrorValue()
            }

        }


        // Action

         function updateStatNbConnectorValue() {
            myIndexingManagementApi.getConnectors(null, true, updateStatNbConnectorValue);
        }

        function updateStatNbOperationValue() {
            myIndexingManagementApi.getOperations(null, 0, 0, true, updateStatNbOperationValue);
        }

         function updateStatNbErrorValue() {
            //TODO when Error management will be implemented
            updateStatNbError($.parseJSON("{'size': 0}"));
        }

        // UI Function

        function updateStatNbConnectorValue(json) {
            $('#statNbConnector').text(json.size);
        }

        function updateStatNbOperationValue(json) {
            $('#statNbOperation').text(json.size);
        }

        function updateStatNbError(json) {
            $('#statNbError').text(json.size);
        }

        return statController;
    }
);
