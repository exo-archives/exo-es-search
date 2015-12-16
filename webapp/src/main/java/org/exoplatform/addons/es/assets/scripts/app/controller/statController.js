/**
 * Created by TClement on 12/15/15.
 */

define('statController', ['SHARED/jquery', 'indexingManagementApi'],
    function($, indexingManagementApi) {

        var statController = function statController() {
            var self = this;

            //Get the Indexing management REST Service API
            var myIndexingManagementApi = new indexingManagementApi();

            self.init = function() {

                //Init stats value
                self.updateStatNbConnector();
                self.updateStatNbOperation();
                self.updateStatNbError();

                //Set refresh interval for operations stats to 5 seconds
                setInterval(function(){
                    self.updateStatNbOperation();
                }, 5000);

                //TODO Set refresh interval for errors stats to 5 seconds

            }

            self.updateStatNbConnector = function() {
                myIndexingManagementApi.getConnectors(null, true, updateStatNbConnectorValue);
            }

            self.updateStatNbOperation = function() {
                myIndexingManagementApi.getOperations(null, 0, 0, true, updateStatNbOperationValue);
            }

            self.updateStatNbError = function() {
                //TODO when Error management will be implemented
                updateStatNbError(0);
            }

            //Update UI Component

            function updateStatNbConnectorValue(json) {
                $('#statNbConnector').text(json.size);
            }

            function updateStatNbOperationValue(json) {
                $('#statNbOperation').text(json.size);
            }

            function updateStatNbError(json) {
                $('#statNbError').text(json.size);
            }

        }

        return statController;
    }
);
