// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac.client;

import io.confluent.kafka.security.authorizer.Action;
import io.confluent.kafka.security.authorizer.AuthorizeResult;
import io.confluent.kafka.security.authorizer.Authorizer;
import io.confluent.security.rbac.client.rest.RbacRestClient;
import io.confluent.security.rbac.utils.JsonMapper;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This is the implementation of {@link Authorizer} which connects to the given metadata service Urls
 * to perform the operations.
 * <p>
 * An instance of RbacRestAuthorizer can be instantiated by passing configuration properties like below.
 * <pre>
 *     Map<String, Object> configs = new HashMap<>();
 *     configs.put(RbacRestClientConfig.BOOTSTRAP_METADATA_SERVER_URLS_PROP, "http://localhost:8080");
 *     Authorizer rbacRestAuthorizer = new RbacRestAuthorizer();
 *     rbacRestAuthorizer.configure(configs);
 * </pre>
 * <p>
 * There are different options available as mentioned in {@link RbacRestClientConfig} like
 * <pre>
 * - {@link RbacRestClientConfig#BOOTSTRAP_METADATA_SERVER_URLS_PROP}.
 * - {@link RbacRestClientConfig#METADATA_SERVER_URL_MAX_AGE_PROP}.
 * - {@link RbacRestClientConfig#BASIC_AUTH_CREDENTIALS_PROVIDER_PROP}.
 * - {@link RbacRestClientConfig#BASIC_AUTH_USER_INFO_PROP}.
 * </pre>
 * <pre>
 * This can be used to authorize list of {@link Action} for a given userPrincipal
 * </pre>
 */
public class RbacRestAuthorizer implements Authorizer {

    private RbacRestClient restClient;

    @Override
    public void configure(final Map<String, ?> configs) {
        restClient = new RbacRestClient(configs);
    }

    @Override
    public List<AuthorizeResult> authorize(final KafkaPrincipal sessionPrincipal,
                                           final String host, final List<Action> actions) {
        try {
            if (restClient == null)
                throw new IllegalStateException("RbacRestClient is not initialized.");

            List<String> results = restClient.authorize(
                    JsonMapper.objectMapper().writeValueAsString(sessionPrincipal),
                    host,
                    actions);
            return  results.stream()
                    .map(AuthorizeResult::valueOf)
                    .collect(Collectors.toList());
        } catch (Exception e) {
           throw new RuntimeException("Error occurred" +
                   " while executing authorize operation", e);
        }
    }

    @Override
    public void close() throws IOException {
        if (restClient != null)
            restClient.close();
    }

}
