
require(['SHARED/jquery', 'statController', 'connectorController', 'operationController', 'appBroadcaster'],
    function($, statController, connectorController, operationController, appBroadcaster){

        //Controller
        var myStatController = new statController();
        var myConnectorController = new connectorController();
        var myOperationController = new operationController();
        //Event broadcaster
        var myAppBroadcaster = new appBroadcaster();

        $(document).ready(
            function($) {

                //Init Stats
                myStatController.init(myAppBroadcaster);

                //Init Connector list
                myConnectorController.init(myAppBroadcaster);

                //Init Operation list
                myOperationController.init(myAppBroadcaster);

            }
        );

    }
);