/*
 * Copyright 2018 Confluent Inc.
 */

package io.confluent.common.security.utils;

import java.security.PublicKey;

import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;

public class CloudUtils {

  private static final String JWT_ISSUER = "Confluent";

  /**
   * returns a common {@link JwtConsumer} for building/validating JWS tokens for
   *  OAuth 2 authentication across CCloud
   * The JWS token must have:
   *  - an issuer `iss` claim representing the principal that issued the token
   *  - a subject `sub` claim representing the principal who will use the token
   *  - a JWT ID `jti` claim which serves as a unique identifier of the token
   *  - a issued at `iat` claim which is the time the token was created
   *  - an expiration time `exp` claim showing the time after which this token
   *    should be considered expired
   */
  public static JwtConsumer createJwtConsumer(PublicKey publicKey) {
    return new JwtConsumerBuilder()
        .setExpectedIssuer(JWT_ISSUER) // whom the JWT needs to have been issued by
        .setVerificationKey(publicKey) // verify the signature with the public key
        .setRequireExpirationTime()
        .setRequireIssuedAt()
        .setRequireJwtId()
        .setRequireSubject()
        .build();
  }
}
