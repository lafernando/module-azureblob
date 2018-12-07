// Copyright (c) 2018 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 Inc. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/config;
import ballerina/test;

Configuration config = {
    accessKey: config:getAsString("ACCESS_KEY"),
    account: config:getAsString("ACCOUNT")
};

Client blobClient = new(config);

@test:Config
function testCreateContainer() {
    _ = blobClient->deleteContainer("ctnx1");
    var result = blobClient->createContainer("ctnx1");
    if (result is error) {
        test:assertFail(msg = <string> result.detail().message);
    }
}

@test:Config {
    dependsOn: ["testCreateContainer"]
}
function testListContainers() {
    var result = blobClient->listBlobContainers();
    if (result is error) {
        test:assertFail(msg = <string> result.detail().message);
    } else {
        test:assertTrue(result.containers.length() > 0);
    }
}

@test:Config {
    dependsOn: ["testListContainers"]
}
function testPutBlob() {
    var result = blobClient->putBlob("ctnx1", "blob1", [1, 2, 3, 4, 5]);
    if (result is error) {
        test:assertFail(msg = <string> result.detail().message);
    }
}

@test:Config {
    dependsOn: ["testPutBlob"]
}
function testGetBlob1() {
    var result = blobClient->getBlob("ctnx1", "blob1");
    if (result is error) {
        test:assertFail(msg = <string> result.detail().message);
    } else {
        var data = result.data;
        if (data is byte[]) {
            test:assertTrue(data[0] == 1);
        } else {
            test:assertFail(msg = "Expeced non-streaming data");
        }
    }
}

@test:Config {
    dependsOn: ["testGetBlob1"]
}
function testGetBlob2() returns error? {
    var result = blobClient->getBlob("ctnx1", "blob1", streaming = true);
    if (result is error) {
        test:assertFail(msg = <string> result.detail().message);
    } else {
        var ch = result.data;
        if (ch is io:ReadableByteChannel) {
            (byte[],int) data = check ch.read(2);
            test:assertTrue(data[0][0] == 1);
            _ = ch.close();
        } else {
            test:assertFail(msg = "Expeced non-streaming data");
        }
    }
    return ();
}

@test:Config {
    dependsOn: ["testGetBlob2"]
}
function testListBlobs() {
    var result = blobClient->listBlobs("ctnx1");
    if (result is error) {
        test:assertFail(msg = <string> result.detail().message);
    } else {
        test:assertTrue(result.blobs[0].name == "blob1");
    }
    return ();
}

@test:Config {
    dependsOn: ["testListBlobs"]
}
function testDeleteBlob() {
    var result = blobClient->deleteBlob("ctnx1", "blob1");
    if (result is error) {
        test:assertFail(msg = <string> result.detail().message);
    }
}


@test:Config {
    dependsOn: ["testDeleteBlob"]
}
function testDeleteContainer() {
    var result = blobClient->deleteContainer("ctnx1");
    if (result is error) {
        test:assertFail(msg = <string> result.detail().message);
    }
}


