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
import ballerina/time;

public function generateCommonHeaders() returns map<string> {
    string apiVersion = "2017-07-29";
    string dateString = generateAzureStorageDateString();
    map<string> headers = { "x-ms-date": dateString, "x-ms-version": apiVersion };
    return headers;
}

public function generateCanonicalizedHeadersString(map<string> headers) returns string {
    // sort headers
    string result = "";
    foreach var (key, value) in headers {
      if (key.indexOf("x-ms-") == 0) {
          result = result + key.toLower().trim() + ":" + value.trim() + "\n";
      }
    }
    return result;
}

public function generateAzureStorageDateString() returns string {
    string DATE_TIME_FORMAT = "EEE, dd MMM yyyy HH:mm:ss z";
    time:Time time = time:currentTime();
    time:Timezone zoneValue = { zoneId: "GMT" };
    time:Time standardTime = new(time.time, zoneValue);
    return standardTime.format(DATE_TIME_FORMAT);
}

public function generateError(http:Response resp) returns error {
    xml errorXml = check resp.getXmlPayload();
    error err = error(errorXml.Code.getTextValue(), { message: errorXml.Message.getTextValue() });
    return err;
}

public function populateAuthorizationHeader(string account, string accessKey, string canonicalizedResource, 
                                            string verb, map<string> headers) {
    string signature = generateAzureStorageServiceSignature(accessKey, canonicalizedResource, verb, headers);
    headers["Authorization"] = "SharedKeyLite " + account + ":" + signature;
}

public function populateRequestHeaders(http:Request req, map<string> headers) {
    foreach var (k, v) in headers {
        req.setHeader(k, v);
    }
}

public function generatePutBlobHeaders(BlobType blobType) returns map<string> {
    map<string> headers = { };
    if (blobType == BLOCK_BLOB) {
        headers["x-ms-blob-type"] = "BlockBlob";
    } else if (blobType == APPEND_BLOB) {
        headers["x-ms-blob-type"] = "AppendBlob";
    } else if (blobType == PAGE_BLOB) {
        headers["x-ms-blob-type"] = "PageBlob";
    }
    foreach var (k, v) in generateCommonHeaders() {
        headers[k] = v;
    }
    return headers;
}

public function generateAzureStorageServiceSignature(string accessKey, string canonicalizedResource, 
                                                     string verb, map<string> headers) returns string {
    string canonicalizedHeaders = generateCanonicalizedHeadersString(headers);
    string? value = headers["Content-Type"];
    string contentType = "";
    if (value is string) {
        contentType = value;
    }
    string contentMD5 = "";
    value = headers["Content-MD5"];
    if (value is string) {
        contentMD5 = value;
    }
    string date = "";
    value = headers["Date"];
    if (value is string) {
        date = value;
    }
    string stringToSign = verb.toUpper() + "\n" +
                          contentMD5 + "\n" + 
                          contentType + "\n" +
                          date + "\n" + 
                          canonicalizedHeaders +   
                          canonicalizedResource;
    return crypto:hmac(stringToSign, accessKey, keyEncoding = crypto:BASE64, crypto:SHA256).base16ToBase64Encode();
}


