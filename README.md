# Ballerina Azure Blob Service Connector

This connector allows to use the Azure Blob service through Ballerina. The following section provide you the details on connector operations.

## Compatibility
| Ballerina Language Version 
| -------------------------- 
| 0.991.0                    


The following sections provide you with information on how to use the Azure Blob Service Connector.

- [Contribute To Develop](#contribute-to-develop)
- [Working with Azure Blob Service Connector actions](#working-with-azure-blob-service-connector)
- [Sample](#sample)

### Contribute To develop

Clone the repository by running the following command 
```shell
git clone https://github.com/lafernando/module-azureblob.git
```

### Working with Azure Blob Service Connector

First, import the `wso2/azureblob` module into the Ballerina project.

```ballerina
import wso2/azureblob;
```

In order for you to use the Azure Blob Service Connector, first you need to create an Azure Blob Service Connector client.

```ballerina
azureblob:Configuration config = {
    accessKey: config:getAsString("ACCESS_KEY"),
    account: config:getAsString("ACCOUNT")
};

azureblob:Client blobClient = new(config);
```

##### Sample

```ballerina
import ballerina/config;
import ballerina/io;
import wso2/azureblob;

azureblob:Configuration config = {
    accessKey: config:getAsString("ACCESS_KEY"),
    account: config:getAsString("ACCOUNT")
};

azureblob:Client blobClient = new(config);

public function main(string... args) returns error? {
    check blobClient->createContainer("ctnx1");
    check blobClient->putBlob("ctnx1", "blob1", [1, 2, 3, 4, 5]);
    var result = blobClient->getBlob("ctnx1", "blob1");
    io:println(result);
}
```
