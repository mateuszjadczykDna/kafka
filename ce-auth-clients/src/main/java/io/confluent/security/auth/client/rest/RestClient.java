// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.client.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.security.authorizer.Action;
import io.confluent.security.auth.client.RestClientConfig;
import io.confluent.security.auth.client.provider.BasicAuthCredentialProvider;
import io.confluent.security.auth.client.provider.BuiltInAuthProviders;
import io.confluent.security.auth.client.provider.BuiltInAuthProviders.BasicAuthCredentialProviders;
import io.confluent.security.auth.client.rest.entities.AuthorizeRequest;
import io.confluent.security.auth.client.rest.entities.AuthorizeResponse;
import io.confluent.security.auth.client.rest.entities.ErrorMessage;
import io.confluent.security.auth.client.rest.exceptions.RestClientException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Rest client for sending RBAC requests to the metadata service.
 */
public class RestClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(RestClient.class);

    private static final int HTTP_CONNECT_TIMEOUT_MS = 60000;
    private static final int HTTP_READ_TIMEOUT_MS = 60000;
    private static final int JSON_PARSE_ERROR_CODE = 50005;

    private static final String AUTHORIZE_END_POINT = "/security/1.0/authorize";
    private static final String ACTIVE_NODES_END_POINT = "/security/1.0/activenodes/%s";

    private static final TypeReference<List<String>> ACTIVE_URLS_RESPONSE_TYPE = new TypeReference<List<String>>() {
    };
    private static final TypeReference<AuthorizeResponse> AUTHORIZE_RESPONSE_TYPE = new TypeReference<AuthorizeResponse>() {
    };

    private static final Map<String, String> DEFAULT_REQUEST_PROPERTIES;
    private static ObjectMapper jsonDeserializer = new ObjectMapper();
    private final Time time;

    static {
        DEFAULT_REQUEST_PROPERTIES = new HashMap<>();
        DEFAULT_REQUEST_PROPERTIES.put("Content-Type", "application/json");
    }

    private final List<String> bootstrapMetadataServerURLs;
    private final int requestTimeout;
    private final int httpRequestTimeout;
    private volatile List<String> activeMetadataServerURLs;
    private final String protocol;

    private SSLSocketFactory sslSocketFactory;
    private BasicAuthCredentialProvider basicAuthCredentialProvider;
    private ScheduledExecutorService urlRefreshscheduler;
    private RequestSender requestSender = new HTTPRequestSender();

    public RestClient(final Map<String, ?> configs) {
        this(configs, Time.SYSTEM);
    }

    public RestClient(final Map<String, ?> configs, final Time time) {
        this.time = time;
        RestClientConfig rbacClientConfig = new RestClientConfig(configs);
        this.bootstrapMetadataServerURLs = rbacClientConfig.getList(RestClientConfig.BOOTSTRAP_METADATA_SERVER_URLS_PROP);
        if (bootstrapMetadataServerURLs.isEmpty())
            throw new ConfigException("Missing required bootstrap metadata server url list.");

        this.protocol = protocol(bootstrapMetadataServerURLs);
        this.requestTimeout = rbacClientConfig.getInt(RestClientConfig.REQUEST_TIMEOUT_MS_CONFIG);
        this.httpRequestTimeout = rbacClientConfig.getInt(RestClientConfig.HTTP_REQUEST_TIMEOUT_MS_CONFIG);

        //set basic auth provider
        String basicAuthProvider = (String) configs.get(RestClientConfig.BASIC_AUTH_CREDENTIALS_PROVIDER_PROP);
        String basicAuthProviderName = basicAuthProvider == null || basicAuthProvider.isEmpty()
                ? BasicAuthCredentialProviders.NONE.name() : basicAuthProvider;
        basicAuthCredentialProvider = BuiltInAuthProviders.loadBasicAuthCredentialProvider(basicAuthProviderName);
        basicAuthCredentialProvider.configure(configs);

        //set ssl socket factory
        if (rbacClientConfig.getString(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG) != null)
            sslSocketFactory = createSslSocketFactory(rbacClientConfig);

        if (rbacClientConfig.getBoolean(RestClientConfig.ENABLE_METADATA_SERVER_URL_REFRESH))
            scheduleMetadataServiceUrlRefresh(rbacClientConfig);
        else
            activeMetadataServerURLs = bootstrapMetadataServerURLs;
    }

    private String protocol(final List<String> bootstrapMetadataServerURLs) {
        try {
            return new URL(bootstrapMetadataServerURLs.get(0)).getProtocol();
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Error while fetching URL protocol", e);
        }
    }

    private void scheduleMetadataServiceUrlRefresh(final RestClientConfig rbacClientConfig) {
        //get active metadata server urls
        try {
            activeMetadataServerURLs = getActiveMetadataServerURLs();
            if (activeMetadataServerURLs.isEmpty())
                throw new ConfigException("Active metadata server url list is empty.");
        } catch (Exception e) {
            throw new RuntimeException("Error while fetching activeMetadataServerURLs.", e);
        }

        //periodic refresh of metadata server urls
        Long metadataServerUrlsMaxAgeMS = rbacClientConfig.getLong(RestClientConfig.METADATA_SERVER_URL_MAX_AGE_PROP);
        urlRefreshscheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true);
            return t;
        });

        class MetadataServerUrlFetcher implements Runnable {
            @Override
            public void run() {
                try {
                    activeMetadataServerURLs = getActiveMetadataServerURLs();
                } catch (Exception e) {
                    log.error("Error while refreshing active metadata server urls, retrying", e);
                    urlRefreshscheduler.schedule(new MetadataServerUrlFetcher(), 100, TimeUnit.MILLISECONDS);
                }
            }
        }

        urlRefreshscheduler.scheduleAtFixedRate(new MetadataServerUrlFetcher(), metadataServerUrlsMaxAgeMS,
                metadataServerUrlsMaxAgeMS, TimeUnit.MILLISECONDS);
    }

    private SSLSocketFactory createSslSocketFactory(final RestClientConfig rbacClientConfig) {
        SslFactory sslFactory = new SslFactory(Mode.CLIENT);
        sslFactory.configure(rbacClientConfig.values());
        return sslFactory.sslContext().getSocketFactory();
    }

    public List<String> authorize(final String userPrincipal, final String host,
                                  final List<Action> actions) throws IOException, RestClientException {
        AuthorizeRequest authorizeRequest = new AuthorizeRequest(userPrincipal, host, actions);
        AuthorizeResponse response = httpRequest(AUTHORIZE_END_POINT,
                "PUT",
                authorizeRequest.toJson().getBytes(StandardCharsets.UTF_8),
                AUTHORIZE_RESPONSE_TYPE);
        return response.authorizeResults;
    }

    public List<String> getActiveMetadataServerURLs() throws IOException, RestClientException {
        UrlSelector urlSelector = new UrlSelector(bootstrapMetadataServerURLs);
        String path = String.format(ACTIVE_NODES_END_POINT, protocol);
        return httpRequest(path, "GET", null, ACTIVE_URLS_RESPONSE_TYPE,
                urlSelector);
    }

    private <T> T httpRequest(String path,
                              String method,
                              byte[] requestBodyData,
                              TypeReference<T> responseFormat) throws IOException, RestClientException {
        UrlSelector urlSelector = new UrlSelector(activeMetadataServerURLs);
        return httpRequest(path, method, requestBodyData, responseFormat, urlSelector);
    }

    private <T> T httpRequest(String path,
                              String method,
                              byte[] requestBodyData,
                              TypeReference<T> responseFormat,
                              UrlSelector urlSelector) throws IOException, RestClientException {
        long begin = time.milliseconds();
        long remainingWaitMs = requestTimeout;
        long elapsed;

        for (int i = 0, n = urlSelector.size(); i < n; i++) {
            String baseUrl = urlSelector.current();
            String requestUrl = buildRequestUrl(baseUrl, path);
            try {
                return requestSender.send(requestUrl,
                        method,
                        requestBodyData,
                        responseFormat,
                        remainingWaitMs);
            } catch (IOException e) {
                urlSelector.fail();
                if (i == n - 1) {
                    throw e; // Raise the exception since we have no more urls to try
                }
            }

            elapsed = time.milliseconds() - begin;
            if (elapsed >= requestTimeout) {
                throw new TimeoutException("Request aborted due to timeout.");
            }
            remainingWaitMs = requestTimeout - elapsed;
        }
        throw new IOException("Internal HTTP retry error"); // Can't get here
    }

    private String buildRequestUrl(String baseUrl, String path) {
        // Join base URL and path, collapsing any duplicate forward slash delimiters
        return baseUrl.replaceFirst("/$", "") + "/" + path.replaceFirst("^/", "");
    }

    private void setupSsl(HttpURLConnection connection) {
        if (connection instanceof HttpsURLConnection && sslSocketFactory != null) {
            ((HttpsURLConnection) connection).setSSLSocketFactory(sslSocketFactory);
        }
    }

    private void setBasicAuthRequestHeader(HttpURLConnection connection) {
        String userInfo;
        if (basicAuthCredentialProvider != null
                && (userInfo = basicAuthCredentialProvider.getUserInfo()) != null) {
            String authHeader = Base64.getEncoder().encodeToString(userInfo.getBytes(StandardCharsets.UTF_8));
            connection.setRequestProperty("Authorization", "Basic " + authHeader);
        }
    }

    public void basicAuthCredentialProvider(final BasicAuthCredentialProvider basicAuthCredentialProvider) {
        this.basicAuthCredentialProvider = basicAuthCredentialProvider;
    }

    public void sslSocketFactory(SSLSocketFactory sslSocketFactory) {
        this.sslSocketFactory = sslSocketFactory;
    }

    void requestSender(RequestSender requestSender) {
        this.requestSender = requestSender;
    }

    @Override
    public void close() {
        if (urlRefreshscheduler != null)
            urlRefreshscheduler.shutdownNow();
    }

    private class HTTPRequestSender implements RequestSender {

        ExecutorService executor = new ThreadPoolExecutor(
                0,
                Integer.MAX_VALUE,
                1,
                TimeUnit.MINUTES,
                new SynchronousQueue<>());

        @Override
        public <T> T send(final String requestUrl, final String method, final byte[] requestBodyData,
                          final TypeReference<T> responseFormat, final long requestTimeout) throws IOException, RestClientException {
            Future<T> f = submit(requestUrl, method, requestBodyData, responseFormat);
            try {
                return f.get(Math.min(requestTimeout, httpRequestTimeout), TimeUnit.MILLISECONDS);
            } catch (Throwable e) {
                if (e instanceof ExecutionException) {
                    e = e.getCause();
                }
                if (e instanceof RestClientException) {
                    throw (RestClientException) e;
                } else if (e instanceof IOException) {
                    throw (IOException) e;
                } else {
                    throw new RuntimeException(e);
                }
            }
        }

        private <T> Future<T> submit(final String requestUrl, final String method, final byte[] requestBodyData,
                                     final TypeReference<T> responseFormat) {
            return executor.submit(() -> {
                String requestData = requestBodyData == null
                        ? "null"
                        : new String(requestBodyData, StandardCharsets.UTF_8);
                log.debug(String.format("Sending %s with input %s to %s", method, requestData, requestUrl));
                HttpURLConnection connection = null;
                try {
                    URL url = new URL(requestUrl);
                    connection = (HttpURLConnection) url.openConnection();

                    connection.setConnectTimeout(HTTP_CONNECT_TIMEOUT_MS);
                    connection.setReadTimeout(HTTP_READ_TIMEOUT_MS);

                    setupSsl(connection);
                    connection.setRequestMethod(method);
                    setBasicAuthRequestHeader(connection);
                    // connection.getResponseCode() implicitly calls getInputStream, so always set to true.
                    // On the other hand, leaving this out breaks nothing.
                    connection.setDoInput(true);

                    for (Map.Entry<String, String> entry : DEFAULT_REQUEST_PROPERTIES.entrySet()) {
                        connection.setRequestProperty(entry.getKey(), entry.getValue());
                    }

                    connection.setUseCaches(false);

                    if (requestBodyData != null) {
                        connection.setDoOutput(true);
                        try (OutputStream os = connection.getOutputStream()) {
                            os.write(requestBodyData);
                            os.flush();
                        } catch (IOException e) {
                            log.error("Failed to send HTTP request to endpoint: " + url, e);
                            throw e;
                        }
                    }

                    int responseCode = connection.getResponseCode();
                    if (responseCode == HttpURLConnection.HTTP_OK) {
                        InputStream is = connection.getInputStream();
                        T result = jsonDeserializer.readValue(is, responseFormat);
                        is.close();
                        return result;
                    } else if (responseCode == HttpURLConnection.HTTP_NO_CONTENT) {
                        return null;
                    } else {
                        InputStream es = connection.getErrorStream();
                        ErrorMessage errorMessage;
                        try {
                            errorMessage = jsonDeserializer.readValue(es, ErrorMessage.class);
                        } catch (JsonProcessingException e) {
                            errorMessage = new ErrorMessage(JSON_PARSE_ERROR_CODE, e.getMessage());
                        }
                        es.close();
                        throw new RestClientException(errorMessage.message(), responseCode,
                                errorMessage.errorCode());
                    }

                } finally {
                    if (connection != null) {
                        connection.disconnect();
                    }
                }
            });
        }
    }

}
