// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.server.plugins.auth.stats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;

public class AuthenticationStats implements AuthenticationStatsMBean {
  private static final AuthenticationStats instance;
  private static final Logger logger =
      LoggerFactory.getLogger(AuthenticationStats.class);

  static {
    instance = new AuthenticationStats();
    MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    String objectName = "io.confluent.kafka.server.plugins:type=Authentication";
    try {
      ObjectName mbeanName = new ObjectName(objectName);
      mbeanServer.registerMBean((AuthenticationStatsMBean) instance, mbeanName);
    } catch (InstanceAlreadyExistsException e) {
      logger.error("Auth stats MBean already exists", e);
    } catch (MBeanRegistrationException e) {
      logger.error("Auth stats MBean registration failed", e);
    } catch (NotCompliantMBeanException e) {
      logger.error("Auth stats MBean not compliant", e);
    } catch (MalformedObjectNameException e) {
      logger.error("Auth stats MBean is malformed: " + objectName, e);
    }
  }

  public static AuthenticationStats getInstance() {
    return instance;
  }

  private AtomicLong succeeded = new AtomicLong(0);
  private AtomicLong failed = new AtomicLong(0);

  private AuthenticationStats() {}

  public void incrSucceeded() {
    succeeded.incrementAndGet();
  }

  public void incrFailed() {
    failed.incrementAndGet();
  }

  // For testing
  public void reset() {
    succeeded.set(0L);
    failed.set(0L);
  }

  @Override
  public long getTotal() {
    return succeeded.get() + failed.get();
  }

  @Override
  public long getSucceeded() {
    return succeeded.get();
  }

  @Override
  public long getFailed() {
    return failed.get();
  }

  @Override
  public String toString() {
    return "AuthenticationStats{"
        + "succeeded=" + succeeded
        + ", failed=" + failed
        + '}';
  }
}
