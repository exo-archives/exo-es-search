/**
 * Created by TClement on 12/14/15.
 */

//export module
if ( typeof define === "function" && define.amd ) {
    define(['jquery'], function($) {
        return ApiClient;
    });
}

var ApiClient = function ApiClient() {
    var self = this;

    self.hostMap = null;
    self.defaultHeaderMap = null;
    self.debugging = null;
    self.basePath = null;
    self.json = null;
    self.authentications = null;
    self.statusCode = null;
    self.responseHeaders = null;
    self.dateFormat = null;

    /**
     * Invoke API by sending HTTP request with the given options.
     *
     * @param path The sub-path of the HTTP URL
     * @param method The request method, one of "GET", "POST", "PUT", and "DELETE"
     * @param queryParams The query parameters
     * @param body The request body object - if it is not binary, otherwise null
     * @param binaryBody The request body object - if it is binary, otherwise null
     * @param headerParams The header parameters
     * @param formParams The form parameters
     * @param accept The request's Accept header
     * @param contentType The request's Content-Type header
     * @param authNames The authentications to apply
     * @return The response body in type of string
     */
    self.invokeAPI = function(path, method, queryParams, body, binaryBody, headerParams, formParams, accept, contentType, authNames) {

        /*
        console.log('path: ' + path);
        console.log('method: ' + method);
        console.log('queryParams: ' + queryParams);
        console.log('body: ' + body);
        console.log('headerParams: ' + headerParams);
        console.log('formParams: ' + formParams);
        */

        var request = {
            url: path,
            type: method,
            contentType: "application/json",
            accepts: "application/json",
            cache: false,
            dataType: 'jsonp',
            data: JSON.stringify(body)/*,
            error: function(jqXHR) {
                console.log("ajax error " + jqXHR.status);
            }*/
        };
        return $.ajax(request);

    }

    /**
     * Escape the given string to be used as URL query value.
     */
    self.escapeString = function(str) {
        return str;
    }


}