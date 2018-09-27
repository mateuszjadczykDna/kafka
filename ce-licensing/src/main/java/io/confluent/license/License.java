/*
 * Copyright [2016  - 2016] Confluent Inc.
 */

package io.confluent.license;

import org.jose4j.jwa.AlgorithmConstraints;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.NumericDate;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.IssValidator;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.jwt.consumer.SubValidator;
import org.jose4j.lang.JoseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.List;

/**
 * this class provides utility functions and a cli for creating/verifying licenses
 *
 * <p>a license is a jwt (https://jwt.io/introduction/) that is
 *   <ul>
 *     <li>issued by "Confluent"</li>
 *     <li>has an expiration time</li>
 *     <li>has a subject</li>
 *   </ul>
 *
 * <p>a jwt license may make a claim about support for "monitoring" (either true or false)
 */
public class License {

  private static final Logger log = LoggerFactory.getLogger(License.class);

  public static JwtClaims baseClaims(String audience, long expiration, boolean monitoring) {
    JwtClaims claims = new JwtClaims();
    claims.setIssuer("Confluent");  // who creates the token and signs it
    claims.setAudience(audience); // to whom the token is intended to be sent
    claims.setExpirationTime(NumericDate.fromMilliseconds(expiration));
    claims.setGeneratedJwtId(); // a unique identifier for the token
    claims.setIssuedAtToNow();  // when the token was issued/created (now)
    claims.setNotBeforeMinutesInThePast(2);
    claims.setSubject("Confluent Enterprise"); // the subject/principal is whom the token is about
    claims.setClaim("monitoring", monitoring);
    return claims;
  }

  public static String sign(PrivateKey key, String audience, long expiration, boolean monitoring)
      throws JoseException {
    JwtClaims claims = baseClaims(audience, expiration, monitoring);
    JsonWebSignature jws = new JsonWebSignature();
    jws.setPayload(claims.toJson());
    jws.setKey(key);
    jws.setAlgorithmHeaderValue(AlgorithmIdentifiers.RSA_USING_SHA256);
    return jws.getCompactSerialization();
  }

  public static JwtClaims verify(PublicKey key, String license) throws InvalidJwtException {
    JwtConsumer jwtConsumer = commonVerifyOptions()
        .setRequireSubject() // the JWT must have a subject claim
        .setExpectedIssuer("Confluent") // whom the JWT needs to have been issued by
        .setVerificationKey(key) // verify the signature with the public key
        .build(); // create the JwtConsumer instance

    return jwtConsumer.processToClaims(license);
  }

  public static JwtClaims verifyStored(PublicKey key, String license) throws InvalidJwtException {
    JwtConsumer jwtConsumer = commonVerifyOptions()
        .setDisableRequireSignature()
        .setSkipVerificationKeyResolutionOnNone()
        .setJwsAlgorithmConstraints(AlgorithmConstraints.NO_CONSTRAINTS)
        .setVerificationKey(key) // verify the signature with the public key
        .build(); // create the JwtConsumer instance

    return jwtConsumer.processToClaims(license);
  }

  private static JwtConsumerBuilder commonVerifyOptions() throws InvalidJwtException {
    return new JwtConsumerBuilder()
        .setSkipAllDefaultValidators()
        .registerValidator(new IssValidator("Confluent", true))
        .registerValidator(new SubValidator(true));
  }

  public static long getExpiration(JwtClaims claims) throws Throwable {
    try {
      NumericDate expirationDate = claims.getExpirationTime();
      return expirationDate != null
             ? expirationDate.getValueInMillis()
             : Long.MIN_VALUE;
    } catch (MalformedClaimException e) {
      log.warn("Unable to extract expiration due to malformed claim: ", e);
      throw e;
    } catch (Throwable t) {
      log.warn("Unable to extract expiration: ", t);
      throw t;
    }
  }

  public static List<String> getAudience(JwtClaims claims) {
    try {
      return claims.getAudience();
    } catch (Throwable t) {
      log.warn("unable to getAudience", t);
      return new ArrayList<>();
    }
  }

  public static boolean verifyMonitoring(JwtClaims claims) {
    try {
      return claims.getClaimValue("monitoring", Boolean.class);
    } catch (MalformedClaimException e) {
      return false;
    } catch (Throwable t) {
      log.warn("unable to verifyMonitoring", t);
      return false;
    }
  }

  public static PrivateKey loadPrivateKey(InputStream is)
      throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
    byte[] keyBytes = readFully(is);
    PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
    return KeyFactory.getInstance("RSA").generatePrivate(spec);
  }

  public static PublicKey loadPublicKey()
      throws NoSuchAlgorithmException, IOException, InvalidKeySpecException {
    try (
        final InputStream keyData =
            License.class.getClassLoader().getResourceAsStream("secrets/public_key.der")
    ) {
      return loadPublicKey(keyData);
    }
  }

  public static PublicKey loadPublicKey(InputStream is)
      throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
    X509EncodedKeySpec xspec = new X509EncodedKeySpec(readFully(is));
    return KeyFactory.getInstance("RSA").generatePublic(xspec);
  }

  private static byte[] readFully(InputStream in) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buffer = new byte[1024];
    int n;
    while ((n = in.read(buffer)) != -1) {
      baos.write(buffer, 0, n);
    }
    return baos.toByteArray();
  }

  // visible for testing
  public static String generateTrialLicense(JwtClaims trialClaims)
      throws InvalidLicenseException {
    JsonWebSignature jws = new JsonWebSignature();
    jws.setAlgorithmConstraints(AlgorithmConstraints.NO_CONSTRAINTS);
    jws.setAlgorithmHeaderValue(AlgorithmIdentifiers.NONE);
    jws.setPayload(trialClaims.toJson());
    try {
      return jws.getCompactSerialization();
    } catch (JoseException e) {
      log.error("Error while attempting to start trial period: ", e);
      throw new InvalidLicenseException(
          "Error creating license for trial version: ", e
      );
    }
  }

}
