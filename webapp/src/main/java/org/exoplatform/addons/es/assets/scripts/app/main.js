
require(['SHARED/jquery', 'controller/statController', 'controller/connectorController', 'controller/operationController'],
    function($, statController, connectorController, operationController){

        //Get the Services
        var myStatController = new statController();
        var myConnectorController = new connectorController();
        var myOperationController = new operationController();

        $(document).ready(
            function($) {

                //Init Stats
                myStatController.init();

                //Init Connector list
                myConnectorController.init();

                //Init Operation list
                myOperationController.init();

            }
        );

    }
);