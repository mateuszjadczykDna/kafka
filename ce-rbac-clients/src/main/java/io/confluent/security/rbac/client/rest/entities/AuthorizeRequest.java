// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac.client.rest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.security.authorizer.Action;

import io.confluent.security.rbac.utils.JsonMapper;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class AuthorizeRequest {

  public final String userPrincipal;
  public final String host;
  public final List<Action> actions;

  @JsonCreator
  public AuthorizeRequest(
      @JsonProperty("userPrincipal") String userPrincipal,
      @JsonProperty("host") String host,
      @JsonProperty("actions") List<Action> actions) {
    this.userPrincipal = userPrincipal;
    this.host = host;
    this.actions = actions;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final AuthorizeRequest that = (AuthorizeRequest) o;
    return Objects.equals(userPrincipal, that.userPrincipal) &&
            Objects.equals(host, that.host) &&
            Objects.equals(actions, that.actions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(userPrincipal, host, actions);
  }

  @Override
  public String toString() {
    return "AuthorizeRequest{" +
            "userPrincipal='" + userPrincipal + '\'' +
            ", host='" + host + '\'' +
            ", actions=" + actions +
            '}';
  }

  public String toJson() throws IOException {
    return JsonMapper.objectMapper().writeValueAsString(this);
  }
}
