/**
 * Created by TClement on 12/15/15.
 */

define('operationController', ['SHARED/jquery', 'indexingManagementApi', 'indexingOperationResource'],
    function($, indexingManagementApi, indexingOperationResource) {

        var operationController = function operationController() {
            var self = this;

            //Get the Indexing management REST Service API
            var myIndexingManagementApi = new indexingManagementApi();

            self.init = function() {

                //Init the Operation list
                self.updateOperationList();
                initBinding();

            }

            self.updateOperationList = function() {
                myIndexingManagementApi.getOperations(null, 0, 10, false, fillOperationTable);
            }

            self.deleteOperation = function(indexingOperationId) {

                myIndexingManagementApi.deleteOperation(indexingOperationId, afterSentDeletingOperation);
            }

        }

        //Update UI Component

        function fillOperationTable(json) {

            //Loop on operations to add one line per Operation in the table
            var html = "";
            for(var i = 0; i < json.resources.length; i++) {

                html += "<tr>" +
                "    <th scope='row'>{{operation.id}}</th>" +
                "    <td>{{operation.entityType}}</td>" +
                "    <td>{{operation.entityId}}</td>" +
                "    <td>{{operation.operation}}</td>" +
                "    <td>{{operation.timestamp.time}}</td>" +
                "    <td>" +
                "        <button type='button'" +
                "                data-operationId=''" +
                "                class='btn-operation-delete btn btn-primary btn-mini'>" +
                "            Delete" +
                "        </button>" +
                "    </td>" +
                "</tr>";
            }

            //Update the table
            $('#indexingOperationTable tbody').html(html);

        }

        function initBinding() {

            //Deleting queue operation Event
            $(document).on('click.btn-operation-delete', '.btn-operation-delete', function () {

                //Get operation Id
                var jOperation = $(this);
                var jOperationId = jOperation.attr('data-operationId');

                //Trigger the deleting operation
                self.deleteOperation(jOperationId);
            });

        }

        function afterSentDeletingOperation() {
            //Here I call all UI component that need to be refresh after sending a deleting operation
            console.log('refresh UI component after Sent Deleting Operation');
        }

        return operationController;
    }
);
