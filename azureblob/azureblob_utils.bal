//
// Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
//

import ballerina/crypto;
import ballerina/encoding;
import ballerina/time;

function generatePutBlobHeaders(BlobType blobType) returns map<string> {
    map<string> headers = { };
    if (blobType == BLOCK_BLOB) {
        headers["x-ms-blob-type"] = "BlockBlob";
    } else if (blobType == APPEND_BLOB) {
        headers["x-ms-blob-type"] = "AppendBlob";
    } else if (blobType == PAGE_BLOB) {
        headers["x-ms-blob-type"] = "PageBlob";
    }
    foreach var (k, v) in generateStorageCommonHeaders() {
        headers[k] = v;
    }
    return headers;
}

function decodeListBlobXML(xml payload) returns ListBlobResult|error {
    BlobInfo?[] blobs = [];
    int index = 0;
    foreach var item in payload.Blobs.Blob {
        if (item is xml) {
            int contentLength = check int.convert(item.Properties["Content-Length"].getTextValue());
            BlobType blobType = BLOCK_BLOB;
            string blobTypeString = item.Properties.BlobType.getTextValue();
            if (blobTypeString == "PageBlob") {
                blobType = PAGE_BLOB;
            } else if (blobTypeString == "AppendBlob") {
                blobType = APPEND_BLOB;
            }
            BlobInfo blob = { name: item.Name.getTextValue(), contentLength: contentLength, blobType: blobType };
            blobs[index] = blob;
            index = index + 1;
        }
    }
    ListBlobResult result = { blobs: blobs };
    return result;
}

function decodeListBlobContainerXML(xml payload) returns ListBlobContainersResult {
    BlobContainerInfo[] containers = [];
    int index = 0;
    foreach var item in payload.Containers.Container {
        if (item is xml) {
            BlobContainerInfo container = { name: item.Name.getTextValue() };
            containers[index] = container;
            index = index + 1;
        }
    }
    ListBlobContainersResult result = { containers: containers };
    return result;
}




