// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/**
 * Hierarchical scopes for role assignments. This is used to scope roles assignments to
 * individual clusters or other levels of scope. It is also used to limit the data cached
 * in embedded authorizers.
 *
 * For example, with a two level scope consisting of root scope "myorg" and clusters
 * "myorg/clusterA" and "myorg/clusterB", roles may be assigned at cluster level for
 * "myorg/clusterA" and "myorg/clusterB". Authorization service providing metadata for
 * all clusters will use the root scope "myorg" to process role assignments of both clusters,
 * while a broker belonging to "myorg/clusterA" only uses role assignments of clusterA.
 */
public class Scope {

  public static final String SCOPE_SEPARATOR = "/";

  private final String name;
  private final Scope parent;

  @JsonCreator
  public Scope(@JsonProperty("name") String name) {
    if (name == null || name.isEmpty())
      throw new IllegalArgumentException("Scope must be non-empty");
    if (name.startsWith(SCOPE_SEPARATOR) || name.endsWith(SCOPE_SEPARATOR))
      throw new IllegalArgumentException("Scope elements must be non-empty");
    this.name = name;
    int index = name.lastIndexOf(SCOPE_SEPARATOR);
    if (index >= 0) {
      this.parent = new Scope(name.substring(0, index));
    } else {
      this.parent = null;
    }
  }

  @JsonProperty
  public String name() {
    return name;
  }

  public Scope parent() {
    return parent;
  }

  public boolean containsScope(Scope o) {
    if (o == null)
      return false;
    else if (this.equals(o))
      return true;
    else
      return containsScope(o.parent);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Scope)) {
      return false;
    }

    Scope that = (Scope) o;
    return Objects.equals(name, that.name) && Objects.equals(parent, that.parent);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, parent);
  }

  @Override
  public String toString() {
    return name;
  }
}
