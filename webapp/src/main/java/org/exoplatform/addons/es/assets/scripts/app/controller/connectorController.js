/**
 * Created by TClement on 12/15/15.
 */

define('connectorController', ['SHARED/jquery', 'indexingManagementApi', 'operationEnum', 'indexingOperationResource', 'operationController', 'statController'],
    function($, indexingManagementApi, operationEnum, indexingOperationResource) {

        //Service
        var myIndexingManagementApi = new indexingManagementApi();
        //Controller
        var myStatController = new statController();
        var myOperationController = new operationController();


        var connectorController = function connectorController() {
            var self = this;

            self.init = function() {

                //Init the connector list
                self.updateConnectorList();
                initUiListener();

            }

            self.updateConnectorList = function() {
                updateConnectorTable();
            }

        }

        //Listener

        function initUiListener() {
            addReindexConnectorUiListener();
        }

        function addReindexConnectorUiListener() {

            //Reindex connector Event
            $(document).on('click.btn-connector-reindex', '.btn-connector-reindex', function () {

                //Get operation type
                var jConnector = $(this);
                var jConnectorType = jConnector.attr('data-connectorType');

                //Trigger the new Indexing Operation
                reindexConnector(jConnectorType);
            });

        }

        // Action

        function updateConnectorTable() {
            myIndexingManagementApi.getConnectors(null, false, fillConnectorTable);
        }

        function reindexConnector(connectorType) {

            //Construct the indexingOperation
            var indexingOperation = new indexingOperationResource();
            indexingOperation.setEntityType(connectorType);
            indexingOperation.setOperation(new operationEnum().REINDEX);

            myIndexingManagementApi.addOperation(indexingOperation, afterSentReindexingOperation);
        }

        //Callback function

        function fillConnectorTable(json) {

            //Loop on connectors to add one line per connector in the table
            var html = "";
            for(var i = 0; i < json.resources.length; i++) {

                var checked = '';
                if (json.resources[i].enable) checked = 'checked';

                html += "<tr>" +
                "    <th scope='row'>" + json.resources[i].type + "</th>" +
                "    <td>" + json.resources[i].description + "</td>" +
                "    <td>" + json.resources[i].index + "</td>" +
                "    <td>" +
                "        <div class='connector-switch'>" +
                "            <input type='checkbox' name='connector-switch' data-connectorType='" + json.resources[i].type + "' " + checked + ">" +
                "        </div>" +
                "    </td>" +
                "    <td>" +
                "        <button data-connectorType='" + json.resources[i].type + "' type='button' class='btn-connector-reindex btn btn-primary btn-mini'>" +
                "           Reindex" +
                "        </button>" +
                "    </td>" +
                "</tr>";
            }

            //Update the table
            $('#indexingConnectorTable tbody').html(html);

        }

        function broadcastReindexConnector() {
            //Here I call all UI component that need to be refresh after sending a reindexing operation
            updateConnectorTable();
            myStatController.updateStatNbOperation();
            myOperationController.updateOperationList();
        }

        return connectorController;
    }
);
