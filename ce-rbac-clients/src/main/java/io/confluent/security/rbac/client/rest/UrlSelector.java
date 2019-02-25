// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac.client.rest;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class UrlSelector {

  private int index;
  private final List<String> urls;

  public UrlSelector(List<String> urls) {
    if (urls == null || urls.isEmpty()) {
      throw new IllegalArgumentException("Expected at least one metadata server URL to be passed in constructor");
    }

    this.urls = new ArrayList<>(urls);
    this.index = new Random().nextInt(urls.size());
  }

  /**
   * Get the current url
   *
   * @return the url
   */
  public String current() {
    return urls.get(index);
  }

  /**
   * Declare the current url as failed. This will cause the urls to
   * rotate, so that the next request will be done against a new url
   * (if one exists).
   *
   */
  public void fail() {
      index = (index + 1) % urls.size();
  }

  public int size() {
    return urls.size();
  }

  public int index() {
    return index;
  }

  @Override
  public String toString() {
    return urls.toString();
  }

}
