
define(['jquery', 'IndexingManagementApi', 'IndexingConnectorResource'],
    function($, IndexingManagementApi, IndexingConnectorResource){

        console.log('*** main.js is initiated');

        //Get IndexingManagementApi
        var myIndexingManagementApi = new IndexingManagementApi();

        //Test get connectors
        console.log('#### Test get Connectors');
        myIndexingManagementApi.getConnectors(null, true, null);

        /*
        //Test get connector
        console.log('#### Test get Connector');
        myIndexingManagementApi.getConnector('wiki', null, null);

        //Test update connector
        console.log('#### Test Update Connector');
        //Create a new Indexing Connector
        var myIndexingConnectorResource = new IndexingConnectorResource();
        myIndexingConnectorResource.setType('wiki');
        myIndexingConnectorResource.setEnable(false);
        myIndexingManagementApi.updateConnector('wiki', myIndexingConnectorResource, null);

        //Test get operations
        console.log('#### Test get Operations');
        myIndexingManagementApi.getOperations(null, 0, 10, true, null);
        */

    });