// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.client.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import io.confluent.security.auth.client.rest.exceptions.RestClientException;

import java.io.IOException;

public interface RequestSender {
    /**
     * @param <T>               The type of the deserialized response to the HTTP request.
     * @param requestUrl        HTTP connection will be established with this url.
     * @param method            HTTP method ("GET", "POST", "PUT", etc.)
     * @param requestBodyData   Bytes to be sent in the request body.
     * @param responseFormat    Expected format of the response to the HTTP request.
     * @param requestTimeout    request timeout
     * @return The deserialized response to the HTTP request, or null if no data is expected.
     */
    <T> T send(String requestUrl, String method, byte[] requestBodyData,
               TypeReference<T> responseFormat, final long requestTimeout) throws IOException, RestClientException;
}
