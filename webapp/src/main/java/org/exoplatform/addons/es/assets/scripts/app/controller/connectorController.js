/**
 * Created by TClement on 12/15/15.
 */

define('connectorController', ['SHARED/jquery', 'indexingManagementApi', 'operationEnum', 'indexingOperationResource'],
    function($, indexingManagementApi, operationEnum, indexingOperationResource) {

        var connectorController = function connectorController() {
            var self = this;

            //Get the Indexing management REST Service API
            var myIndexingManagementApi = new indexingManagementApi();

            self.init = function() {

                //Init the connector list
                self.updateConnectorList();
                initBinding();

            }

            self.updateConnectorList = function() {
                myIndexingManagementApi.getConnectors(null, false, fillConnectorTable);
            }

            self.reindexConnector = function(indexingOperation) {

                myIndexingManagementApi.addOperation(indexingOperation, afterSentReindexingOperation);
            }

            //Update UI Component

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

            function initBinding() {

                //Reindex connector Event
                $(document).on('click.btn-connector-reindex', '.btn-connector-reindex', function () {

                    //Get operation type
                    var jConnector = $(this);
                    var jConnectorType = jConnector.attr('data-connectorType');

                    //Construct the indexingOperation
                    var indexingOperation = new indexingOperationResource();
                    indexingOperation.setEntityType(jConnectorType);
                    indexingOperation.setOperation(new operationEnum().REINDEX);

                    //Trigger the new Indexing Operation
                    self.reindexConnector(indexingOperation);
                });

            }

            function afterSentReindexingOperation() {
                //Here I call all UI component that need to be refresh after sending a reindexing operation
                console.log('refresh UI component after Sent Reindexing Operation');
            }

        }

        return connectorController;
    }
);
