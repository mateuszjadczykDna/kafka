// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider.ldap;

import io.confluent.kafka.security.authorizer.provider.ProviderFailedException;
import io.confluent.security.auth.provider.ldap.LdapAuthorizerConfig.SearchMode;
import io.confluent.kafka.common.utils.RetryBackoff;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.Control;
import javax.naming.ldap.HasControls;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.PagedResultsControl;
import javax.naming.ldap.PagedResultsResponseControl;
import javax.naming.ldap.Rdn;
import javax.security.auth.Subject;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.authenticator.LoginManager;
import org.apache.kafka.common.security.kerberos.KerberosLogin;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LdapGroupManager {

  private static final Logger log = LoggerFactory.getLogger(LdapGroupManager.class);

  private static final int CLOSE_TIMEOUT_MS = 30000;

  // Entry change notification from https://www.ietf.org/proceedings/50/I-D/ldapext-psearch-03.txt
  private enum ChangeType {
    ADD(1),
    DELETE(2),
    MODIFY(4),
    RENAME(8),
    UNKNOWN(-1);
    final int value;

    ChangeType(int value) {
      this.value = value;
    }
  }

  private final LdapAuthorizerConfig config;
  private final Time time;
  private final Subject subject;
  private final Map<String, Set<String>> userGroupCache;
  private final ScheduledExecutorService executorService;
  private final ResultEntryConfig resultEntryConfig;
  private final SearchControls searchControls;
  private final RetryBackoff retryBackoff;
  private final AtomicLong failureStartMs;
  private final AtomicInteger retryCount;
  private final AtomicBoolean alive;
  private final PersistentSearch persistentSearch;

  private volatile LdapContext context;
  private volatile Future<?> searchFuture;

  public LdapGroupManager(LdapAuthorizerConfig config, Time time) {
    this.config = config;
    this.time = time;
    this.subject = login();
    this.userGroupCache = new ConcurrentHashMap<>();
    persistentSearch = config.persistentSearch ? new PersistentSearch() : null;

    this.searchControls = new SearchControls();
    switch (config.searchMode) {
      case GROUPS:
        this.searchControls.setReturningAttributes(new String[]{
            config.groupNameAttribute,
            config.groupMemberAttribute
        });
        resultEntryConfig = new ResultEntryConfig(
            config.groupNameAttribute,
            config.groupNameAttributePattern,
            config.groupMemberAttribute,
            config.groupMemberAttributePattern);
        this.searchControls.setSearchScope(config.groupSearchScope);
        break;
      case USERS:
        this.searchControls.setReturningAttributes(new String[]{
            config.userNameAttribute,
            config.userMemberOfAttribute
        });
        resultEntryConfig = new ResultEntryConfig(
            config.userNameAttribute,
            config.userNameAttributePattern,
            config.userMemberOfAttribute,
            config.userMemberOfAttributePattern);
        this.searchControls.setSearchScope(config.userSearchScope);
        break;
      default:
        throw new IllegalArgumentException("Unsupported search mode " + config.searchMode);
    }

    this.alive = new AtomicBoolean(true);
    this.failureStartMs = new AtomicLong(0);
    this.retryCount = new AtomicInteger(0);
    this.retryBackoff = new RetryBackoff(config.retryBackoffMs, config.retryMaxBackoffMs);

    this.executorService = Executors.newSingleThreadScheduledExecutor(runnable -> {
      final Thread thread = new Thread(runnable, "ldap-group-manager");
      thread.setDaemon(true);
      return thread;
    });

    log.info("LDAP group manager created with config: {}", config);
  }

  /**
   * Starts the LDAP group manager and schedules either periodic or persistent search. In both cases
   * an initial non-persistent search is performed to initialize the cache to ensure that the cache
   * is populated before returning from `start()`.
   * <p>
   * For periodic search, a new search is scheduled every refresh interval to refresh the cache
   * from LDAP.
   * </p><p>
   * Even when persistent search is enabled, a non-persistent search is used for initialization
   * because we cannot detect when all the existing entries have been processed by a persistent
   * search since `enumeration.next()` just blocks waiting for changes.
   *
   * Persistent search is performed using changesOnly=false. So all entries are read a second time
   * when the persistent search is initiated. This is to avoid the timing window if entries are
   * updated in between the first non-persistent search and the persistent search request. If the
   * overhead of a second read during `start()` turns out to be an issue, we will need to initiate
   * the async persistent search with `changesOnly=true` on the scheduler thread and invoke the
   * synchrononous non-persistent search on the thread invoking `start()` after that. Note that we
   * would need to make processing of search results thread-safe. We would also need to
   * re-populate the cache using non-persistent search on connection failures in the same way.
   * </p>
   */
  public void start() {
    log.trace("Starting LDAP group manager");
    // Do one search synchronously to initialize the cache, retrying if necessary
    boolean done = false;
    do {
      try {
        searchAndProcessResults();
        done = true;
      } catch (Throwable e) {
        try {
          if (failed()) {
            throw e;
          }
          int backoffMs = processFailureAndGetBackoff(e);
          Thread.sleep(backoffMs);
        } catch (Throwable t) {
          throw new LdapAuthorizerException("Ldap group manager initialization failed", t);
        }
      }
    } while (!done);

    if (config.persistentSearch) {
      schedulePersistentSearch(0, false);
    } else {
      schedulePeriodicSearch(config.refreshIntervalMs, config.refreshIntervalMs);
    }
  }

  public void close() {
    alive.set(false);
    if (searchFuture != null) {
      searchFuture.cancel(true);
    }
    executorService.shutdownNow();
    try {
      executorService.awaitTermination(CLOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      log.debug("LdapGroupManager.close() was interrupted", e);
    }
    try {
      if (context != null) {
        context.close();
      }
    } catch (NamingException e) {
      log.debug("Could not close LDAP context", e);
    }
  }

  public Set<String> groups(String userPrincipal) {
    if (failed())
      throw new ProviderFailedException("LDAP Group provider has failed");
    return userGroupCache.getOrDefault(userPrincipal, Collections.emptySet());
  }

  public boolean failed() {
    long failedMs = failureStartMs.get();
    return failedMs != 0 && time.milliseconds() > failedMs + config.retryTimeoutMs;
  }

  private void resetFailure() {
    if (retryCount.getAndSet(0) != 0) {
      log.info("LDAP search succeeded, resetting failed status");
    }
    failureStartMs.set(0);
  }

  private int processFailureAndGetBackoff(Throwable exception) {
    if (!alive.get()) {
      return 0;
    }
    log.error("LDAP search failed", exception);
    try {
      if (searchFuture != null) {
        searchFuture.cancel(false);
      }
      if (context != null) {
        context.close();
      }
    } catch (Exception e) {
      log.error("Context could not be closed", e);
    }
    context = null;
    if (failureStartMs.get() == 0) {
      failureStartMs.set(time.milliseconds());
    }
    return retryBackoff.backoffMs(retryCount.getAndIncrement());
  }

  private void persistentSearch() throws NamingException, IOException {
    if (context == null) {
      createLdapContext();
    }
    try {
      context.setRequestControls(new Control[]{persistentSearch.control});
      searchControls.setTimeLimit(0);
    } catch (Exception e) {
      throw new LdapAuthorizerException("Request controls could not be created");
    }
    log.trace("Starting persistent search");
    NamingEnumeration<SearchResult> enumeration = search(searchControls);
    resetFailure();

    // Process persistent search results until read times out due to no LDAP modifications
    // or connection fails.
    while (alive.get()) {
      processSearchResults(enumeration);
    }
  }

  /**
   * Schedules a new persistent search that processes changes until read times out due
   * to no modifications or the connection to the LDAP server is lost. A new search is
   * initiated whenever a search fails.
   * <p>
   * Apache DS has a timing window between processing existing entries and setting up
   * the persistent listener to process modifications. To workaround this issue, we always
   * process existing entries using a non-persistent search whenever a new search is started.
   * </p>
   */
  private void schedulePersistentSearch(long initialDelayMs, boolean initializeCache) {
    log.trace("Scheduling persistent search, initialDelayMs={}, initializeCache={}",
        initialDelayMs, initializeCache);
    searchFuture = executorService.schedule(() -> {
      try {
        if (initializeCache) {
          searchAndProcessResults();
        }
        persistentSearch();
      } catch (Throwable e) {
        int backoffMs = processFailureAndGetBackoff(e);
        schedulePersistentSearch(backoffMs, true);
      }
    }, initialDelayMs, TimeUnit.MILLISECONDS);
  }

  private void schedulePeriodicSearch(long initialDelayMs, long refreshIntervalMs) {
    log.trace("Scheduling periodic search with initialDelayMs={}, refreshIntervalMs {}",
        initialDelayMs, refreshIntervalMs);
    searchFuture = executorService.scheduleWithFixedDelay(() -> {
      try {
        searchAndProcessResults();
      } catch (Throwable e) {
        int backoffMs = processFailureAndGetBackoff(e);
        schedulePeriodicSearch(backoffMs, config.refreshIntervalMs);
      }
    }, initialDelayMs, refreshIntervalMs, TimeUnit.MILLISECONDS);
  }

  void searchAndProcessResults() throws NamingException, IOException {
    if (context == null) {
      createLdapContext();
      maybeSetPagingControl(null);
    }
    Set<String> currentSearchEntries = new HashSet<>();
    byte[] cookie = null;
    do {
      NamingEnumeration<SearchResult> enumeration = search(searchControls);
      currentSearchEntries.addAll(processSearchResults(enumeration));
      resetFailure();
      Control[] responseControls = context.getResponseControls();
      if (config.searchPageSize > 0 && responseControls != null) {
        for (Control responseControl : responseControls) {
          if (responseControl instanceof PagedResultsResponseControl) {
            PagedResultsResponseControl pc = (PagedResultsResponseControl) responseControl;
            cookie = pc.getCookie();
            log.debug("Search returned page, totalSize {}", pc.getResultSize());
            break;
          } else {
            log.debug("Ignoring response control {}", responseControl);
          }
        }
      }
      maybeSetPagingControl(cookie);
    } while (cookie != null);

    removeDeletedEntries(currentSearchEntries);
    log.debug("Search completed, group cache is {}", userGroupCache);
  }

  private void removeDeletedEntries(Set<String> currentSearchEntries) {
    Set<String> prevSearchEntries = new HashSet<>();
    if (config.searchMode == SearchMode.USERS) {
      prevSearchEntries.addAll(userGroupCache.keySet());
    } else {
      userGroupCache.values().forEach(prevSearchEntries::addAll);
    }
    prevSearchEntries.stream()
        .filter(name -> !currentSearchEntries.contains(name))
        .forEach(this::processSearchResultDelete);
  }

  private NamingEnumeration<SearchResult> search(SearchControls searchControls)
      throws NamingException {
    if (config.searchMode == SearchMode.GROUPS) {
      log.trace("Searching groups with base {} filter {}: ",
          config.groupSearchBase, config.groupSearchFilter);
      return context.search(config.groupSearchBase, config.groupSearchFilter, searchControls);
    } else {
      log.trace("Searching users with base {} filter {}: ",
          config.userSearchBase, config.userSearchFilter);
      return context.search(config.userSearchBase, config.userSearchFilter, searchControls);
    }
  }

  /**
   * Process the results of an LDAP search.
   * <ul>
   *   <li>For persistent search, this method blocks waiting for updates. It fails with
   *   a {@link NamingException} if the search fails (e.g. connection to LDAP server is lost)</li>
   *   <li>For non-persistent search, this method processes the entries in the current search
   *   and returns the current set of entries. The returned entries are used to determine
   *   deleted entries that need to be removed from the cache.</li>
   * </ul>
   * @param enumeration Enumeration of search results
   * @return entries added in the current search if this is a non-persistent search,
   *     empty set otherwise.
   */
  private Set<String> processSearchResults(NamingEnumeration<SearchResult> enumeration)
      throws NamingException {
    Set<String> currentSearchEntries = new HashSet<>();
    while (enumeration.hasMore()) {
      SearchResult searchResult = enumeration.next();
      log.trace("Processing search result {}", searchResult);
      ResultEntry resultEntry = searchResultEntry(searchResult);
      if (resultEntry == null) {
        continue;
      }
      Control changeResponseControl = null;
      if (config.persistentSearch && searchResult instanceof HasControls) {
        Control[] controls = ((HasControls) searchResult).getControls();
        for (Control control : controls) {
          if (persistentSearch.isEntryChangeResponseControl(control)) {
            changeResponseControl = control;
            log.debug("Entry change search response control {}", control);
          } else {
            log.debug("Ignoring search response control {}", control);
          }
        }
      }

      ChangeType changeType = changeResponseControl != null
          ? persistentSearch.changeType(changeResponseControl) : ChangeType.MODIFY;
      switch (changeType) {
        case ADD:
        case MODIFY:
          if (!config.persistentSearch) {
            currentSearchEntries.add(resultEntry.name);
          }
          processSearchResultModify(resultEntry);
          break;
        case DELETE:
          processSearchResultDelete(resultEntry.name);
          break;
        case RENAME:
          String previousDn = persistentSearch.previousDn(changeResponseControl);
          Pattern pattern = config.searchMode == SearchMode.GROUPS ? config.groupDnNamePattern :
              config.userDnNamePattern;
          String previousName = null;
          if (pattern == null) {
            List<Rdn> rdns = new LdapName(previousDn).getRdns();
            for (Rdn rdn : rdns) {
              if (resultEntryConfig.nameAttribute.equals(rdn.getType())) {
                previousName = (String) rdn.getValue();
              }
            }
          } else {
            previousName = attributeValue(previousDn, pattern, "", "rename entry");
          }

          if (previousName != null) {
            processSearchResultDelete(previousName);
          }
          processSearchResultModify(resultEntry);
          break;
        default:
          throw new IllegalArgumentException("Unsupported response control type " + changeType);
      }
      if (config.persistentSearch) {
        log.debug("Group cache after change notification is {}", userGroupCache);
      }
    }
    return currentSearchEntries;
  }

  private void processSearchResultModify(ResultEntry resultEntry) {
    if (resultEntry != null) {
      if (config.searchMode == SearchMode.GROUPS) {
        String group = resultEntry.name;
        Set<String> members = resultEntry.members;
        for (String user : members) {
          Set<String> groups = userGroupCache.computeIfAbsent(user, u -> new HashSet<>());
          groups.add(group);
        }
        for (Map.Entry<String, Set<String>> entry : userGroupCache.entrySet()) {
          String user = entry.getKey();
          Set<String> userGroups = entry.getValue();
          if (userGroups.contains(group) && !members.contains(user)) {
            userGroups.remove(group);
            if (userGroups.isEmpty()) {
              userGroupCache.remove(user);
            }
          }
        }
      } else {
        String user = resultEntry.name;
        Set<String> groups = resultEntry.members;
        userGroupCache.put(user, groups);
      }
    }
  }

  private void processSearchResultDelete(String name) {
    if (config.searchMode == SearchMode.GROUPS) {
      processSearchResultModify(new ResultEntry(name, Collections.emptySet()));
    } else {
      userGroupCache.remove(name);
    }
  }

  private ResultEntry searchResultEntry(SearchResult searchResult) throws NamingException {
    Attributes attributes = searchResult.getAttributes();
    Attribute nameAttr = attributes.get(resultEntryConfig.nameAttribute);
    if (nameAttr != null) {
      String name = attributeValue(nameAttr.get(), resultEntryConfig.nameAttributePattern,
          "", "search result");
      if (name == null) {
        return null;
      }
      Set<String> members = new HashSet<>();
      Attribute memberAttr = attributes.get(resultEntryConfig.memberAttribute);
      if (memberAttr != null) {
        NamingEnumeration<?> attrs = memberAttr.getAll();
        while (attrs.hasMore()) {
          Object member = attrs.next();
          String memberName = attributeValue(member, resultEntryConfig.memberAttributePattern,
              name, "member");
          if (memberName != null) {
            members.add(memberName);
          }
        }
      }
      return new ResultEntry(name, members);
    }
    return null;
  }

  private void maybeSetPagingControl(byte[] cookie) {
    try {
      if (config.searchPageSize > 0) {
        Control control = new PagedResultsControl(config.searchPageSize, cookie, cookie != null);
        context.setRequestControls(new Control[]{control});
      }
    } catch (IOException | NamingException e) {
      log.warn("Paging control could not be set", e);
    }
  }

  private String attributeValue(Object value, Pattern pattern, String parent, String attrDesc) {
    if (value == null) {
      log.error("Ignoring null {} in LDAP {} {}", attrDesc, config.searchMode, parent);
      return null;
    }
    if (pattern == null) {
      return String.valueOf(value);
    }
    Matcher matcher = pattern.matcher(value.toString());
    if (!matcher.matches()) {
      log.error("Ignoring {} in LDAP {} {} that doesn't match pattern: {}",
          attrDesc, config.searchMode, parent, value);
      return null;
    }
    return matcher.group(1);
  }

  private Subject login() {
    String jaasConfigProp = LdapAuthorizerConfig.CONFIG_PREFIX + SaslConfigs.SASL_JAAS_CONFIG;
    Password jaasConfig = (Password) config.values().get(jaasConfigProp);
    String authProp = LdapAuthorizerConfig.CONFIG_PREFIX + Context.SECURITY_AUTHENTICATION;

    // If JAAS config is provided, login regardless of authentication type
    // For GSSAPI, login using either JAAS config prop or default Configuration
    // from the login context `KafkaServer`.
    if (jaasConfig == null && !"GSSAPI".equals(config.originals().get(authProp))) {
      return new Subject();
    } else {
      try {
        JaasContext jaasContext = jaasContext(jaasConfig, "GSSAPI");

        Map<String, Object> loginConfigs = new HashMap<>();
        for (Map.Entry<String, ?> entry : config.values().entrySet()) {
          String name = entry.getKey();
          Object value = entry.getValue();
          if (name.startsWith(LdapAuthorizerConfig.CONFIG_PREFIX) && value != null) {
            loginConfigs
                .put(name.substring(LdapAuthorizerConfig.CONFIG_PREFIX.length()), value);
          }
        }
        LoginManager loginManager = LoginManager.acquireLoginManager(jaasContext, "GSSAPI",
            KerberosLogin.class, loginConfigs);
        return loginManager.subject();
      } catch (Exception e) {
        String configSource = jaasConfig != null
            ? LdapAuthorizerConfig.CONFIG_PREFIX + SaslConfigs.SASL_JAAS_CONFIG
            : "static JAAS configuration";
        throw new LdapAuthorizerException("Login using " + configSource + " failed", e);
      }
    }
  }

  private void createLdapContext() throws IOException, NamingException {
    Hashtable<String, String> env = config.ldapContextEnvironment;
    this.context = Subject.doAs(subject, (PrivilegedAction<InitialLdapContext>) () -> {
      try {
        return new InitialLdapContext(env, null);
      } catch (NamingException e) {
        throw new LdapAuthorizerException(
            "LDAP context could not be created with provided configs", e);
      }
    });
  }

  public static JaasContext jaasContext(Password jaasConfig, String mechanism) throws Exception {
    // Configuration sources in order of precedence
    //   1) JAAS configuration option: ldap.authorizer.gssapi.sasl.jaas.config
    //   2) static Configuration ldap.KafkaServer
    //   3) static Configuration KafkaServer
    ListenerName listenerName = new ListenerName("ldap"); // only for static context name
    Map<String, Object> configs = jaasConfig == null ? Collections.emptyMap() :
        Collections.singletonMap(mechanism.toLowerCase(Locale.ROOT) + "."
            + SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
    return JaasContext.loadServerContext(listenerName, mechanism, configs);
  }

  private static class ResultEntryConfig {

    final String nameAttribute;
    final Pattern nameAttributePattern;
    final String memberAttribute;
    final Pattern memberAttributePattern;

    ResultEntryConfig(String nameAttribute,
        Pattern nameAttributePattern,
        String memberAttribute,
        Pattern memberAttributePattern) {
      this.nameAttribute = nameAttribute;
      this.nameAttributePattern = nameAttributePattern;
      this.memberAttribute = memberAttribute;
      this.memberAttributePattern = memberAttributePattern;
    }
  }

  private static class ResultEntry {
    final String name;
    final Set<String> members;

    ResultEntry(String name, Set<String> members) {
      this.name = name;
      this.members = members;
    }
  }

  /**
   * Encapsulate access to persistent control classes `com.sun.jndi.ldap.PersistentSearchControl`
   * and `com.sun.jndi.ldap.EntryChangeResponseControl` using reflection to enable running with
   * JDK9 and above. Persistent search is performed using `changesOnly=false`. See {@link #start()}
   * for details.
   */
  private static class PersistentSearch {
    final Control control;
    private Class<? extends Control> entryChangeResponseControlClass;
    private final Method changeTypeMethod;
    private final Method previousDnMethod;


    PersistentSearch() {
      try {
        Class<? extends Control> controlClass = Utils.loadClass(
            "com.sun.jndi.ldap.PersistentSearchControl", Control.class);
        Constructor<? extends Control> constructor =
            controlClass.getConstructor(int.class, boolean.class, boolean.class, boolean.class);
        // changeTypes=ANY, changesOnly=false, returnControls=true, criticality=true;
        control = constructor.newInstance(0xf, false, true, true);

        entryChangeResponseControlClass = Utils.loadClass(
            "com.sun.jndi.ldap.EntryChangeResponseControl", Control.class);
        changeTypeMethod = entryChangeResponseControlClass.getMethod("getChangeType");
        previousDnMethod = entryChangeResponseControlClass.getMethod("getPreviousDN");
      } catch (Exception e) {
        throw new ConfigException("Persistent search could not be enabled", e);
      }
    }

    ChangeType changeType(Control changeResponseControl) {
      try {
        Integer changeTypeValue = (Integer) changeTypeMethod.invoke(changeResponseControl);
        for (ChangeType type : ChangeType.values()) {
          if (type.value == changeTypeValue) {
            return type;
          }
        }
        return ChangeType.UNKNOWN;
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new LdapAuthorizerException("Could not get change type", e);
      }
    }

    String previousDn(Control changeResponseControl) {
      try {
        return (String) previousDnMethod.invoke(changeResponseControl);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new LdapAuthorizerException("Could not get change type", e);
      }
    }

    boolean isEntryChangeResponseControl(Control control) {
      return entryChangeResponseControlClass.isInstance(control);
    }
  }
}
