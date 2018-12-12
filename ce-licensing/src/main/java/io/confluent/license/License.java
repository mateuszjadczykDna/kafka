/*
 * Copyright [2016  - 2016] Confluent Inc.
 */

package io.confluent.license;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.jose4j.jwa.AlgorithmConstraints;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.NumericDate;
import org.jose4j.jwt.ReservedClaimNames;
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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import io.confluent.license.util.StringUtils;

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
public final class License {

  private static final Logger log = LoggerFactory.getLogger(License.class);
  private static final String DATE_FORMAT_STR = "yyyy-MM-dd";
  static final String MONITORING_CLAIM = "monitoring";
  static final String TYPE_CLAIM_NAME = "licenseType";
  private static final Collection<String> RELEVANT_CLAIM_NAMES = Collections.unmodifiableList(
      Arrays.asList(
          ReservedClaimNames.AUDIENCE,
          ReservedClaimNames.ISSUER,
          ReservedClaimNames.SUBJECT,
          ReservedClaimNames.EXPIRATION_TIME,
          MONITORING_CLAIM,
          TYPE_CLAIM_NAME
      )
  );

  public static JwtClaims baseClaims(String audience, long expiration, boolean monitoring) {
    JwtClaims claims = new JwtClaims();
    claims.setIssuer("Confluent");  // who creates the token and signs it
    claims.setAudience(audience); // to whom the token is intended to be sent
    claims.setExpirationTime(NumericDate.fromMilliseconds(expiration));
    claims.setGeneratedJwtId(); // a unique identifier for the token
    claims.setIssuedAtToNow();  // when the token was issued/created (now)
    claims.setNotBeforeMinutesInThePast(2);
    claims.setSubject("Confluent Enterprise"); // the subject/principal is whom the token is about
    claims.setClaim(MONITORING_CLAIM, monitoring);
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
      return expiration(claims);
    } catch (MalformedClaimException e) {
      log.warn("Unable to extract expiration due to malformed claim: ", e);
      throw e;
    } catch (Throwable t) {
      log.warn("Unable to extract expiration: ", t);
      throw t;
    }
  }

  protected static long expiration(JwtClaims claims) throws Throwable {
    NumericDate expirationDate = claims.getExpirationTime();
    return expirationDate != null
           ? expirationDate.getValueInMillis()
           : Long.MIN_VALUE;
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

  private final JwtClaims jwtClaims;
  private final Time clock;
  private final String serialized;

  /**
   * Create a license with the supplied {@link JwtClaims JWT Claims}.
   *
   * @param claims the JWT claims
   * @param clock the clock that can be used to compute the {@link #expirationDate()};
   *              may not be null
   * @param serialized the serialized form of the license; may not be null
   */
  public License(JwtClaims claims, Time clock, String serialized) {
    this.jwtClaims = claims;
    this.clock = clock;
    this.serialized = serialized;
  }

  /**
   * Get the subject of the license.
   *
   * @return the subject, may be null or empty if the license is malformed
   */
  public String subject() {
    try {
      return jwtClaims.getSubject();
    } catch (MalformedClaimException e) {
      return "";
    }
  }

  /**
   * Determine whether the license represents a trial, such that the
   * {@link #audienceString() audience} is simply "trial".
   *
   * @return true if the license is know to be a trial, or false otherwise
   */
  public boolean isTrial() {
    return hasLicenseType("trial") || "trial".equalsIgnoreCase(audienceString());
  }

  /**
   * Determine whether the license represents a free tier license that allows for
   * single-node clusters only.
   *
   * @return true if the license represents the free tier, or false otherwise
   */
  public boolean isFreeTier() {
    return hasLicenseType("free");
  }

  /**
   * Get the license's expiration date.
   *
   * @return the expiration date; never null
   */
  public Date expirationDate() {
    return new Date(expirationMillis());
  }

  /**
   * Get the license's expiration date as a formatted string of the form {@value DATE_FORMAT_STR}.
   *
   * @return the string representation of the {@link #expirationDate()}; never null
   */
  public String expirationDateString() {
    final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STR);
    return dateFormat.format(expirationDate());
  }

  /**
   * Determine whether this license has expired.
   *
   * @return true if the license is expired, or false otherwise
   */
  public boolean isExpired() {
    return !isFreeTier() && timeRemaining(TimeUnit.MILLISECONDS) <= 0;
  }

  /**
   * Determine whether this license is currently valid.
   *
   * @return true if the license is valid, or false otherwise
   */
  public boolean isValid() {
    return !isExpired();
  }

  /**
   * Determine if this license is equivalent to the specified license, using the
   * {@link JwtClaims#getAudience() audience},
   * {@link JwtClaims#getExpirationTime() expiration time},
   * {@link JwtClaims#getIssuer() issuer},
   * {@link JwtClaims#getSubject() subject},
   * and monitoring claim.
   * This method does not consider the {@link JwtClaims#getIssuedAt() issued timestamp},
   * {@link JwtClaims#getNotBefore() not before}, and {@link JwtClaims#getJwtId() JWT ID} claims.
   *
   * @param other the other license
   * @return true if this license and {@code other} are equivalent, or false otherwise
   */
  public boolean isEquivalentTo(License other) {
    return hasMatchingClaims(other, TYPE_CLAIM_NAME);
  }

  /**
   * Determine if this license is a strict renewal of the specified license. This is essentially
   * the same logic as {@link #isEquivalentTo(License)} except the
   * {@link ReservedClaimNames#EXPIRATION_TIME expiration time} of this license must be later than
   * the specified license.
   *
   * @param other the other license
   * @return true if {@code other} is a renewal of the {@code previous} license
   */
  public boolean isRenewalOf(License other) {
    return hasMatchingClaims(other, ReservedClaimNames.EXPIRATION_TIME, TYPE_CLAIM_NAME)
           && other != null
           && other.expiresBefore(this);
  }

  /**
   * Get the amount of time in the specified units that remains before the license expires.
   *
   * @param unit the unit for the returned time; may not be null
   * @return the time remaining before the license expires; may be negative if the license is
   *         already expired
   */
  public long timeRemaining(TimeUnit unit) {
    final long timeRemainingMillis = expirationMillis() - clock.milliseconds();
    return unit.convert(timeRemainingMillis, TimeUnit.MILLISECONDS);
  }

  /**
   * Get the audience for the license.
   * This corresponds to the customer information, or "trial" if the license is a trial.
   *
   * @return the audience string(s); may be empty if the license has no valid audience
   */
  public List<String> audience() {
    return License.getAudience(jwtClaims);
  }

  /**
   * Get a string with comma-separated {@link #audience() audience literals} for the license.
   * This corresponds to the customer information, or "trial" if the license is a trial.
   *
   * @return the string containing the comma-separated audience literals; may be empty if the
   *         license has no valid audience
   */
  public String audienceString() {
    return Utils.join(audience(), ",");
  }

  /**
   * Determine if this license expires before the specified license.
   *
   * @param other the other license; may be null
   * @return true if the other license is null or this license expires before the other license
   */
  public boolean expiresBefore(License other) {
    if (other == null) {
      return false;
    }
    return this.expirationMillis() < other.expirationMillis();
  }

  @Override
  public int hashCode() {
    return serialized.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof License) {
      License that = (License) obj;
      // Compare all claims ...
      return this.jwtClaims().equals(that.jwtClaims());
    }
    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (isFreeTier()) {
      sb.append("Free Tier license ");
    } else if (isTrial()) {
      sb.append("Trial license ");
    } else {
      sb.append("License ");
    }
    if (!StringUtils.isBlank(subject())) {
      sb.append("for ")
        .append(subject());
    }
    try {
      if (isExpired()) {
        sb.append(" expired on ")
          .append(expirationDateString());
      } else if (!isFreeTier()) {
        long remainingMillis = timeRemaining(TimeUnit.MILLISECONDS);
        long daysRemaining = TimeUnit.MILLISECONDS.toDays(remainingMillis);
        sb.append(" expires in ")
          .append(daysRemaining)
          .append(" days on ")
          .append(expirationDateString());
      }
    } catch (Throwable t) {
      sb.append(" with invalid expiration");
    }
    sb.append(".");
    return sb.toString();
  }

  JwtClaims jwtClaims() {
    return jwtClaims;
  }

  String serializedForm() {
    return serialized;
  }

  protected long expirationMillis() {
    try {
      return expiration(jwtClaims);
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  protected boolean hasLicenseType(String expected) {
    try {
      return expected.equalsIgnoreCase(jwtClaims().getStringClaimValue(TYPE_CLAIM_NAME));
    } catch (MalformedClaimException e) {
      return false;
    }
  }

  /**
   * Determine if this license matches all of the {@link #RELEVANT_CLAIM_NAMES relevant claims}
   * in the supplied license, except for any excluded claims.
   *
   * @param other the other license
   * @param excludeClaims the names of claims that should be excluded from the check; may be empty
   * @return true if {@code other} has claims that match this license, or false otherwise or if
   *         {@code other} is null
   */
  protected boolean hasMatchingClaims(License other, String... excludeClaims) {
    if (other == this) {
      return true;
    }
    if (other == null) {
      return false;
    }

    // Compare only the relevant claims, except any exclusions ...
    Set<String> exclusions = new HashSet<>(Arrays.asList(excludeClaims));
    JwtClaims thisClaims = this.jwtClaims();
    JwtClaims thatClaims = other.jwtClaims();
    for (String claimName : RELEVANT_CLAIM_NAMES) {
      if (exclusions.contains(claimName)) {
        continue;
      }
      Object prevClaim = thisClaims.getClaimValue(claimName);
      Object laterClaim = thatClaims.getClaimValue(claimName);
      if (!Objects.equals(prevClaim, laterClaim)) {
        return false;
      }
    }
    return true;
  }
}
