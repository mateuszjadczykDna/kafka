// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.server.plugins.auth.stats;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

public class TenantAuthenticationStats {
  private static final Logger logger =
      LoggerFactory.getLogger(TenantAuthenticationStats.class);

  static final String MBEAN_NAME =
      "io.confluent.kafka.server.plugins:type=TenantAuthentication";

  // Even though only a small number of characters are disallowed in JMX, quote any
  // string containing special characteres to be safe.
  private static final Pattern MBEAN_PATTERN = Pattern.compile("[\\w-%\\. \t]*");

  private static final TenantAuthenticationStats instance = new TenantAuthenticationStats();

  private final Map<MultiTenantPrincipal, TenantAuthStats> registeredMBeans =
      new HashMap<>();

  public static TenantAuthenticationStats instance() {
    return instance;
  }

  public void onSuccessfulAuthentication(MultiTenantPrincipal principal) {
    mbean(principal).succeeded.incrementAndGet();
  }

  public synchronized void removeAllMBeans() {
    for (Iterator<MultiTenantPrincipal> it = registeredMBeans.keySet().iterator(); it.hasNext();) {
      MultiTenantPrincipal principal = it.next();
      unregisterMBean(principal);
      it.remove();
    }
  }

  public synchronized void removeUnusedMBeans(Set<MultiTenantPrincipal> validPrincipals) {
    for (Iterator<MultiTenantPrincipal> it = registeredMBeans.keySet().iterator(); it.hasNext();) {
      MultiTenantPrincipal principal = it.next();
      if (!validPrincipals.contains(principal)) {
        unregisterMBean(principal);
        it.remove();
      }
    }
  }

  private synchronized TenantAuthStats mbean(MultiTenantPrincipal principal) {
    TenantAuthStats mbean = registeredMBeans.get(principal);
    if (mbean == null) {
      mbean = registerMBean(principal);
    }
    return mbean;
  }

  private ObjectName mbeanName(MultiTenantPrincipal principal)
      throws MalformedObjectNameException {
    String name = String.format("%s,tenant=%s,user=%s", MBEAN_NAME,
        quoteIfRequired(principal.tenantMetadata().tenantName),
        quoteIfRequired(principal.user()));
    return new ObjectName(name);
  }

  String quoteIfRequired(String str) {
    return MBEAN_PATTERN.matcher(str).matches() ? str : ObjectName.quote(str);
  }

  private synchronized TenantAuthStats registerMBean(MultiTenantPrincipal principal) {
    MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    TenantAuthStats mbean = new TenantAuthStats();
    try {
      ObjectName mbeanName = mbeanName(principal);
      mbeanServer.registerMBean(mbean, mbeanName);
      registeredMBeans.put(principal, mbean);
    } catch (InstanceAlreadyExistsException e) {
      logger.error("Auth stats MBean already exists for " + principal, e);
    } catch (MBeanRegistrationException |  NotCompliantMBeanException
        | MalformedObjectNameException e) {
      logger.error("Auth stats MBean could not be registered for " + principal, e);
    }
    return mbean;
  }

  private synchronized void unregisterMBean(MultiTenantPrincipal principal) {
    MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    try {
      ObjectName mbeanName = mbeanName(principal);
      mbeanServer.unregisterMBean(mbeanName);
    } catch (InstanceNotFoundException e) {
      logger.warn("Auth stats MBean not found for " + principal, e);
    } catch (MBeanRegistrationException | MalformedObjectNameException e) {
      logger.error("Auth stats MBean could not be unregistered for " + principal, e);
    }
  }

  public interface TenantAuthStatsMBean {
    long getSucceeded();
  }

  public static class TenantAuthStats implements TenantAuthStatsMBean {

    final AtomicLong succeeded = new AtomicLong();

    @Override
    public long getSucceeded() {
      return succeeded.get();
    }
  }
}
