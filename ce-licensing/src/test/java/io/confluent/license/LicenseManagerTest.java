/*
 * Copyright [2017  - 2017] Confluent Inc.
 */

package io.confluent.license;

import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.lang.JoseException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.util.concurrent.TimeUnit;

import static io.confluent.license.License.baseClaims;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.powermock.api.easymock.PowerMock.mockStaticPartial;

@RunWith(PowerMockRunner.class)
@PrepareForTest({License.class, LicenseStore.class, LicenseManager.class})
@PowerMockIgnore("javax.management.*")
public class LicenseManagerTest extends EasyMockSupport {

  private static final Logger log = LoggerFactory.getLogger(LicenseManager.class);
  private static final String TOPIC = "_confluent-command";
  private static final KeyPair KEY_PAIR = generateKeyPair();

  private static final String VALID_REGULAR_LICENSE = sign(
      KEY_PAIR.getPrivate(),
      Time.SYSTEM.milliseconds() + TimeUnit.DAYS.toMillis(360)
  );

  private static final String EXPIRED_REGULAR_LICENSE = sign(
      KEY_PAIR.getPrivate(),
      Time.SYSTEM.milliseconds() - TimeUnit.DAYS.toMillis(5)
  );

  private Time time = Time.SYSTEM;
  @Mock
  private LicenseStore store;

  private LicenseManager manager;

  public static KeyPair generateKeyPair() {
    try {
      KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
      keyGen.initialize(2048);
      return keyGen.genKeyPair();
    } catch (NoSuchAlgorithmException e) {
      // This can happen for two reasons:
      //  1. The public key was not packaged with the jar.
      //  2. The public key which was packaged was corrupt.
      throw new IllegalStateException("Internal license validation error", e);
    }
  }

  public static String sign(PrivateKey key, long expiration) {
    try {
      return License.sign(key, null, expiration, true);
    } catch (JoseException e) {
      log.error("Error creating license for test");
    }
    return null;
  }

  @Before
  public void setUp() throws Exception {
    store = createMock(LicenseStore.class);
  }

  @Test
  public void testStartStop() throws Exception {
    store.start();
    expectLastCall();
    store.stop();
    expectLastCall();

    replayAll();

    manager = new LicenseManager(TOPIC, store, time);
    manager.stop();

    verifyAll();
  }

  @Test
  public void testStartTrialPeriod() throws Exception {
    store.start();
    expectLastCall();
    expect(store.licenseScan()).andReturn(null);
    store.registerLicense(anyString());
    expectLastCall();
    store.stop();
    expectLastCall();

    replayAll();

    manager = new LicenseManager(TOPIC, store, time);
    manager.registerOrValidateLicense("");
    manager.stop();

    verifyAll();
  }

  @Test(expected = InvalidLicenseException.class)
  public void testWithInvalidLicenseKey() throws Exception {
    store.start();
    expectLastCall();

    replayAll();

    manager = new LicenseManager(TOPIC, store, time);
    manager.registerOrValidateLicense("malarkey license");

    verifyAll();
  }

  @Test
  public void testAlreadyStartedTrialPeriod() throws Exception {
    JwtClaims stillValidTrial = baseClaims(
        "trial",
        time.milliseconds() + TimeUnit.DAYS.toMillis(4) + 1000,
        true
    );
    String trialLicense = License.generateTrialLicense(stillValidTrial);
    store.start();
    expectLastCall();
    expect(store.licenseScan()).andReturn(trialLicense);
    store.stop();
    expectLastCall();

    replayAll();

    manager = new LicenseManager(TOPIC, store, time);
    manager.registerOrValidateLicense("");
    manager.stop();

    verifyAll();
  }

  @Test(expected = InvalidLicenseException.class)
  public void testExpiredTrialPeriod() throws Exception {
    JwtClaims expiredTrial = baseClaims(
        "trial",
        time.milliseconds() - TimeUnit.DAYS.toMillis(4),
        true
    );
    String trialLicense = License.generateTrialLicense(expiredTrial);
    store.start();
    expectLastCall();
    expect(store.licenseScan()).andReturn(trialLicense);

    replayAll();

    manager = new LicenseManager(TOPIC, store, time);
    manager.registerOrValidateLicense("");

    verifyAll();
  }

  @Test
  public void testRegularLicense() throws Exception {
    // PowerMock does not work with EasyMockSupport. Need to explicitly call EasyMock methods and
    // mocks
    mockStaticPartial(License.class, "loadPublicKey");
    store = PowerMock.createMock(LicenseStore.class);
    store.start();
    EasyMock.expectLastCall();
    EasyMock.expect(License.loadPublicKey()).andReturn(KEY_PAIR.getPublic());
    EasyMock.expect(store.licenseScan()).andReturn(null);
    store.registerLicense(anyString());
    EasyMock.expectLastCall();
    store.stop();
    EasyMock.expectLastCall();

    PowerMock.replayAll();

    manager = new LicenseManager(TOPIC, store, time);
    manager.registerOrValidateLicense(VALID_REGULAR_LICENSE);
    manager.stop();

    PowerMock.verifyAll();
  }

  @Test(expected = InvalidLicenseException.class)
  public void testExpiredRegularLicense() throws Exception {
    // PowerMock does not work with EasyMockSupport. Need to explicitly call EasyMock methods and
    // mocks
    mockStaticPartial(License.class, "loadPublicKey");
    store = PowerMock.createMock(LicenseStore.class);
    store.start();
    EasyMock.expectLastCall();
    EasyMock.expect(License.loadPublicKey()).andReturn(KEY_PAIR.getPublic());
    EasyMock.expect(store.licenseScan()).andReturn(EXPIRED_REGULAR_LICENSE);

    PowerMock.replayAll();

    manager = new LicenseManager(TOPIC, store, time);
    manager.registerOrValidateLicense(EXPIRED_REGULAR_LICENSE);

    PowerMock.verifyAll();
  }

}


