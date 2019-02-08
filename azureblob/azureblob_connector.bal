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

import ballerina/http;
import wso2/azurecommons;

# Object to initialize the connection with Azure Blob Service.
#
# + accessKey - The Azure access key
# + account   - The Azure container account name
public type Client client object {

    public string accessKey;
    public string account;

    public function __init(Configuration config) {
        self.accessKey = config.accessKey;
        self.account = config.account;
    }

    # Lists all the blob containers in the current account.
    # + return - If successful, returns `ListBlobContainersResult`, else returns an `error` value
    public remote function listBlobContainers() returns ListBlobContainersResult|error;

    # Lists the blobs in given specific container.
    # + container - The blob container
    # + return - If successful, returns `ListBlobResult`, else returns an `error` value
    public remote function listBlobs(string container) returns ListBlobResult|error;

    # Creates a container.
    # + container - The blob container
    # + return - If successful, returns `()`, else returns an `error` object
    public remote function createContainer(string container) returns error?;

    # Puts a blob to a given container.
    # + container - The blob container
    # + name - The name of the blob entry
    # + data - The blob data content
    # + blobType - The type of the blob as a `BlobType` value
    # + contentType - The content type of the blob data
    # + contentEncoding - The content encoding of the blob data
    # + return - If successful, returns `()`, else returns an `error` object   
    public remote function putBlob(string container, string name, byte[] data, BlobType blobType = "BLOCK",
                            string contentType = "application/octet-stream", 
                            string contentEncoding = "") returns error?;

    # Gets a blob from a given container and blob entry
    # + container - The blob container
    # + name - The name of the blob entry
    # + startRange - An offset into the blob data to be retrieved
    # + endRange - The end limit index of the blob data to be retrieved
    # + streaming - If true, BlobResult.data will be a streaming `ReadableByteChannel` or else, it will be a `byte[]`
    # + return - If successful, returns a `BlobResult` object, else returns an `error` object   
    public remote function getBlob(string container, string name, int startRange = 0, int endRange = 0, 
                            boolean streaming = false) returns BlobResult|error;

    # Deletes a blob entry.
    # + container - The blob container
    # + name - The name of the blob entry
    # + return - If successful, returns `()`, else returns an `error` object
    public remote function deleteBlob(string container, string name) returns error?;

    # Deletes a container.
    # + container - The blob container
    # + return - If successful, returns `()`, else returns an `error` object
    public remote function deleteContainer(string container) returns error?;

};

public remote function Client.listBlobContainers() returns ListBlobContainersResult|error {
    http:Client clientEP = new("https://" + self.account + "." + AZURE_BLOB_SERVICE_DOMAIN);
    string verb = "GET";
    map<string> headers = azurecommons:generateStorageCommonHeaders();
    string canonicalizedResource = "/" + check http:encode(self.account, "UTF8") + "/?comp=list";
    check azurecommons:populateSharedKeyLiteStorageAuthorizationHeader(self.account, self.accessKey, canonicalizedResource, verb, headers);

    http:Request req = new;
    azurecommons:populateRequestHeaders(req, headers);

    var resp = clientEP->get("/?comp=list", message = req);

    if (resp is http:Response) {
        int statusCode = resp.statusCode;
        if (statusCode != http:OK_200) {
            return azurecommons:generateStorageError(resp);
        }
        return decodeListBlobContainerXML(check resp.getXmlPayload());
    } else {
        return resp;
    }
}

public remote function Client.listBlobs(string container) returns ListBlobResult|error {
    http:Client clientEP = new("https://" + self.account + "." + AZURE_BLOB_SERVICE_DOMAIN);
    string verb = "GET";
    map<string> headers = azurecommons:generateStorageCommonHeaders();
    string canonicalizedResource = "/" + check http:encode(self.account, "UTF8") + "/" + 
                                         check http:encode(container, "UTF8") + "?comp=list";
    check azurecommons:populateSharedKeyLiteStorageAuthorizationHeader(self.account, self.accessKey, canonicalizedResource, verb, headers);

    http:Request req = new;
    azurecommons:populateRequestHeaders(req, headers);

    var resp = clientEP->get("/" + untaint container + "?restype=container&comp=list", message = req);

    if (resp is http:Response) {
        int statusCode = resp.statusCode;
        if (statusCode != http:OK_200) {
            return azurecommons:generateStorageError(resp);
        }
        return check decodeListBlobXML(check resp.getXmlPayload());
    } else {
        return resp;
    }
}

public remote function Client.createContainer(string container) returns error? {
    http:Client clientEP = new("https://" + self.account + "." + AZURE_BLOB_SERVICE_DOMAIN);
    string verb = "PUT";
    map<string> headers = azurecommons:generateStorageCommonHeaders();
    string canonicalizedResource = "/" + check http:encode(self.account, "UTF8") + "/" + 
                                         check http:encode(container, "UTF8");
    check azurecommons:populateSharedKeyLiteStorageAuthorizationHeader(self.account, self.accessKey, canonicalizedResource, verb, headers);

    http:Request req = new;
    azurecommons:populateRequestHeaders(req, headers);

    var resp = clientEP->put("/" + untaint container + "?restype=container", req);

    if (resp is http:Response) {
        int statusCode = resp.statusCode;
        if (statusCode != http:CREATED_201) {
            return azurecommons:generateStorageError(resp);
        }
        return ();
    } else {
        return resp;
    }
}

public remote function Client.deleteContainer(string container) returns error? {
    http:Client clientEP = new("https://" + self.account + "." + AZURE_BLOB_SERVICE_DOMAIN);
    string verb = "DELETE";
    map<string> headers = azurecommons:generateStorageCommonHeaders();
    string canonicalizedResource = "/" + check http:encode(self.account, "UTF8") + "/" + 
                                         check http:encode(container, "UTF8");
    check azurecommons:populateSharedKeyLiteStorageAuthorizationHeader(self.account, self.accessKey, canonicalizedResource, verb, headers);

    http:Request req = new;
    azurecommons:populateRequestHeaders(req, headers);

    var resp = clientEP->delete("/" + untaint container + "?restype=container", req);

    if (resp is http:Response) {
        int statusCode = resp.statusCode;
        if (statusCode != http:ACCEPTED_202) {
            return azurecommons:generateStorageError(resp);
        }
        return ();
    } else {
        return resp;
    }
}

public remote function Client.putBlob(string container, string name, byte[] data, BlobType blobType = "BLOCK",
                        string contentType = "application/octet-stream", 
                        string contentEncoding = "") returns error? {
    http:Client clientEP = new("https://" + self.account + "." + AZURE_BLOB_SERVICE_DOMAIN, 
                               config = { chunking: http:CHUNKING_NEVER });

    string verb = "PUT";
    map<string> headers = generatePutBlobHeaders(blobType);
    headers["Content-Type"] = contentType;
    headers["Content-Length"] = "" + data.length();
    if (contentEncoding != "") {
        headers["Content-Encoding"] = contentEncoding;
    }
    string canonicalizedResource = "/" + check http:encode(self.account, "UTF8") + "/" + 
                                         check http:encode(container, "UTF8") + "/" +
                                         check http:encode(name, "UTF-8");
    check azurecommons:populateSharedKeyLiteStorageAuthorizationHeader(self.account, self.accessKey, canonicalizedResource, verb, headers);

    http:Request req = new;
    req.setBinaryPayload(untaint data);
    azurecommons:populateRequestHeaders(req, headers);

    var resp = clientEP->put("/" + untaint container + "/" + untaint name, req);

    if (resp is http:Response) {
        int statusCode = resp.statusCode;
        if (statusCode != http:CREATED_201) {
            return azurecommons:generateStorageError(resp);
        }
        return ();
    } else {
        return resp;
    }
}

public remote function Client.getBlob(string container, string name, int startRange = 0, int endRange = 0, 
                        boolean streaming = false) returns BlobResult|error {
    http:Client clientEP = new("https://" + self.account + "." + AZURE_BLOB_SERVICE_DOMAIN);

    string verb = "GET";
    map<string> headers = azurecommons:generateStorageCommonHeaders();
    string canonicalizedResource = "/" + check http:encode(self.account, "UTF8") + "/" + 
                                         check http:encode(container, "UTF8") + "/" +
                                         check http:encode(name, "UTF-8");
    check azurecommons:populateSharedKeyLiteStorageAuthorizationHeader(self.account, self.accessKey, canonicalizedResource, verb, headers);

    http:Request req = new;
    azurecommons:populateRequestHeaders(req, headers);

    var resp = clientEP->get("/" + untaint container + "/" + untaint name, message = req);

    if (resp is http:Response) {
        int statusCode = resp.statusCode;
        if (statusCode != http:OK_200) {
            return azurecommons:generateStorageError(resp);
        }
        string cls = resp.getHeader("Content-Length");
        BlobInfo blobInfo = { name: name, contentLength: check int.convert(cls), blobType: BLOCK_BLOB };
        return { data: streaming ? check resp.getByteChannel() : check resp.getBinaryPayload(), blobInfo: blobInfo };
    } else {
        return resp;
    }    
}

public remote function Client.deleteBlob(string container, string name) returns error? {
    http:Client clientEP = new("https://" + self.account + "." + AZURE_BLOB_SERVICE_DOMAIN);

    string verb = "DELETE";
    map<string> headers = azurecommons:generateStorageCommonHeaders();
    string canonicalizedResource = "/" + check http:encode(self.account, "UTF8") + "/" + 
                                         check http:encode(container, "UTF8") + "/" +
                                         check http:encode(name, "UTF-8");
    check azurecommons:populateSharedKeyLiteStorageAuthorizationHeader(self.account, self.accessKey, canonicalizedResource, verb, headers);

    http:Request req = new;
    azurecommons:populateRequestHeaders(req, headers);

    var resp = clientEP->delete("/" + untaint container + "/" + untaint name, req);

    if (resp is http:Response) {
        int statusCode = resp.statusCode;
        if (statusCode != http:ACCEPTED_202) {
            return azurecommons:generateStorageError(resp);
        }
        return ();
    } else {
        return resp;
    }
}

# Azure Blob Service configuration.
# + accessKey - The Azure access key
# + account   - The Azure container account name
public type Configuration record {
    string accessKey;
    string account;
};


