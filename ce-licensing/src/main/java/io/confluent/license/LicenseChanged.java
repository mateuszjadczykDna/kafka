/*
 * Copyright [2018  - 2018] Confluent Inc.
 */

package io.confluent.license;

import java.util.function.Consumer;

/**
 * An event signaling that the license has changed.
 *
 * @see LicenseManager#addListener(Consumer)
 * @see LicenseManager#removeListener(Consumer)
 */
public interface LicenseChanged {

  /**
   * The type of license change.
   */
  enum Type {

    /**
     * The license has expired.
     */
    EXPIRED,

    /**
     * The license has been updated with more changes than just the expiration date.
     */
    UPDATED,

    /**
     * The license has been renewed, meaning the new license is identical to the previous license
     * except that the expiration date has been extended.
     */
    RENEWAL
  }

  /**
   * Get the license.
   *
   * @return the license; never null
   */
  License license();

  /**
   * The type of change in the license.
   *
   * @return the type of change; never null
   */
  Type type();

  /**
   * Get a description of this license, perhaps including a reason why this license is used.
   *
   * @return the description of this license; never null
   */
  String description();
}
