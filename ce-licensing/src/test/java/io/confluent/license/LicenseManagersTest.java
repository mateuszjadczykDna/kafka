/*
 * Copyright [2018  - 2018] Confluent Inc.
 */

package io.confluent.license;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.annotation.Mock;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class LicenseManagersTest extends EasyMockSupport {

  private LicenseManagers managers;
  private LicenseManagers.RunningLicenseManager runningManager;
  private LicenseManagers.RunningLicenseManager runningManager2;
  private License license;
  private LicenseManagerTest.MockTime clock;
  @Mock
  private ScheduledExecutorService schedulerService;
  @Mock
  private LicenseManager manager;
  @Mock
  private ScheduledFuture<Object> future;
  @Mock
  private LicenseManager manager2;
  @Mock
  private ScheduledFuture<Object> future2;

  @Before
  public void setup() {
    schedulerService = mock(ScheduledExecutorService.class);
    manager = mock(LicenseManager.class);
    future = mock(ScheduledFuture.class);
    manager2 = mock(LicenseManager.class);
    future2 = mock(ScheduledFuture.class);
    clock = new LicenseManagerTest.MockTime();

    // Use the mock scheduler service
    managers = new LicenseManagers(() -> schedulerService);
  }

  @Test
  public void shouldAllowStartingAndRestartingLicenseManager() {
    // Registering a new license manager will schedule its runner every 24 hours
    expectDoStart(manager);
    expectScheduleAtFixedRate(future);

    replayAll();

    // Start once
    runningManager = (LicenseManagers.RunningLicenseManager) managers.start(manager);
    assertSame(manager, runningManager.licenseManager);
    assertSame(future, runningManager.future);

    // Starting again will return same future
    assertSame(runningManager, managers.start(manager));

    // Ensure no shutdown
    assertTrue(managers.isRunning());

    verifyAll();
  }

  @Test
  public void shouldIgnoreStoppingLicenseManagerThatWasNotStarted() {
    // Registering a new license manager will schedule its runner every 24 hours
    expectDoStart(manager);
    expectScheduleAtFixedRate(future);

    replayAll();

    // Start once
    runningManager = (LicenseManagers.RunningLicenseManager) managers.start(manager);
    assertSame(manager, runningManager.licenseManager);
    assertSame(future, runningManager.future);

    // Starting again will return same future
    assertSame(runningManager, managers.start(manager));

    // Ensure no shutdown
    assertTrue(managers.isRunning());

    // Stop a manager that was not yet started
    assertFalse(managers.stop(manager2));

    verifyAll();
  }

  @Test
  public void shouldNotShutdownExecutorIfNotStoppingAllLicenseManagers() {
    // Registering a new license manager will schedule its runner every 24 hours
    expectDoStart(manager);
    expectScheduleAtFixedRate(future);
    // Registering a 2nd license manager will schedule its runner every 24 hours
    expectDoStart(manager2);
    expectScheduleAtFixedRate(future2);
    // Stopping the second license manager
    expect(future2.cancel(true)).andReturn(true);
    expectDoStop(manager2);
    // Do not expect shutdown since 1st manager remains

    replayAll();

    runningManager = (LicenseManagers.RunningLicenseManager) managers.start(manager);
    assertSame(manager, runningManager.licenseManager);
    assertSame(future, runningManager.future);

    runningManager2 = (LicenseManagers.RunningLicenseManager) managers.start(manager2);
    assertSame(manager2, runningManager2.licenseManager);
    assertSame(future2, runningManager2.future);

    assertTrue(managers.stop(manager2));
    assertTrue(managers.isRunning());

    // Stop a manager that was already stopped
    assertFalse(managers.stop(manager2));
    assertFalse(managers.stop(null));

    verifyAll();
  }

  @Test
  public void shouldStartAndStopLicenseManagerAndShutdownExecutorWithNoMoreManagers() {
    // Registering a new license manager will schedule its runner every 24 hours
    expectDoStart(manager);
    expectScheduleAtFixedRate(future);
    // Stopping a license manager
    expect(future.cancel(true)).andReturn(true);
    expectDoStop(manager);
    // Shutting down executor
    expectShutdownNow();

    replayAll();

    runningManager = (LicenseManagers.RunningLicenseManager) managers.start(manager);
    assertSame(manager, runningManager.licenseManager);
    assertSame(future, runningManager.future);

    assertTrue(managers.stop(manager));

    // Stop a manager that was already stopped
    assertFalse(managers.stop(manager));

    // Ensure shutdown
    assertFalse(managers.isRunning());

    // Stop a manager that was already stopped, should not restart
    assertFalse(managers.stop(manager));
    assertFalse(managers.isRunning());

    verifyAll();
  }

  @Test
  public void shouldStartLicenseManagerAfterShuttingDownExecutor() {
    // Registering a new license manager will schedule its runner every 24 hours
    expectDoStart(manager);
    expectScheduleAtFixedRate(future);
    // Stopping a license manager
    expect(future.cancel(true)).andReturn(true);
    expectDoStop(manager);
    // Shutting down executor
    expectShutdownNow();

    // Registering a new license manager will schedule its runner every 24 hours
    expectDoStart(manager);
    expectScheduleAtFixedRate(future);
    // Stopping a license manager
    expect(future.cancel(true)).andReturn(true);
    expectDoStop(manager);
    // Shutting down executor
    expectShutdownNow();

    replayAll();

    // Register
    runningManager = (LicenseManagers.RunningLicenseManager) managers.start(manager);
    assertSame(manager, runningManager.licenseManager);
    assertSame(future, runningManager.future);

    // Stop
    assertTrue(managers.stop(manager));

    // Ensure shutdown
    assertFalse(managers.isRunning());

    // Register
    runningManager = (LicenseManagers.RunningLicenseManager) managers.start(manager);
    assertSame(manager, runningManager.licenseManager);
    assertSame(future, runningManager.future);

    // Stop
    assertTrue(managers.stop(manager));

    // Ensure shutdown
    assertFalse(managers.isRunning());

    verifyAll();
  }

  @Test
  public void shouldDoNothingWhenShuttingDownWithNoManagers() {
    replayAll();

    // Shutdown
    assertFalse(managers.isRunning());
    managers.shutdown();
    assertFalse(managers.isRunning());

    verifyAll();
  }

  @Test
  public void shouldStopAllLicenseManagersDuringShutdown() {
    // Registering a new license manager will schedule its runner every 24 hours
    expectDoStart(manager);
    expectScheduleAtFixedRate(future);
    // Registering a new license manager will schedule its runner every 24 hours
    expectDoStart(manager2);
    expectScheduleAtFixedRate(future2);

    // Stopping both license managers
    expect(future.cancel(true)).andReturn(true);
    expectDoStop(manager);
    expect(future2.cancel(true)).andReturn(true);
    expectDoStop(manager2);
    // Shutting down executor
    expectShutdownNow();

    replayAll();

    // Register
    assertFalse(managers.isRunning());
    runningManager = (LicenseManagers.RunningLicenseManager) managers.start(manager);
    assertSame(manager, runningManager.licenseManager);
    assertSame(future, runningManager.future);

    // Register
    runningManager2 = (LicenseManagers.RunningLicenseManager) managers.start(manager2);
    assertSame(manager2, runningManager2.licenseManager);
    assertSame(future2, runningManager2.future);

    // Shutdown
    assertTrue(managers.isRunning());
    managers.shutdown();
    assertFalse(managers.isRunning());

    verifyAll();
  }

  @Test
  public void shouldScheduleRunningLicenseManagerUponConstruction() {
    expectScheduleAtFixedRate(future);

    replayAll();

    AtomicReference<LicenseManager> cancelled = new AtomicReference<>();
    runningManager = new LicenseManagers.RunningLicenseManager(
        manager,
        schedulerService,
        cancelled::set
    );

    verifyAll();
  }

  @Test
  public void shouldCalculateNextLicenseCheck() {
    // Submit the standard check every 24 hours
    expectScheduleAtFixedRate(future);

    replayAll();

    AtomicReference<LicenseManager> cancelled = new AtomicReference<>();
    runningManager = new LicenseManagers.RunningLicenseManager(
        manager,
        schedulerService,
        cancelled::set
    );
    assertNull(cancelled.get());

    asserNextLicenseCheck(1, TimeUnit.MINUTES, 15, TimeUnit.SECONDS);
    asserNextLicenseCheck(4, TimeUnit.MINUTES, 15, TimeUnit.SECONDS);
    asserNextLicenseCheck(5, TimeUnit.MINUTES, 5, TimeUnit.MINUTES);
    asserNextLicenseCheck(59, TimeUnit.MINUTES, 5, TimeUnit.MINUTES);
    asserNextLicenseCheck(60, TimeUnit.MINUTES, 1, TimeUnit.HOURS);
    asserNextLicenseCheck(1, TimeUnit.HOURS, 1, TimeUnit.HOURS);
    asserNextLicenseCheck(23, TimeUnit.HOURS, 1, TimeUnit.HOURS);

    asserNextLicenseCheck(-1, TimeUnit.MINUTES, 15, TimeUnit.SECONDS);

    assertNull(cancelled.get());

    verifyAll();
  }

  @Test
  public void shouldSubmitNextLicenseCheck() throws InvalidLicenseException {
    // Generate a license that expires 50 days from now
    license = new License(
        License.baseClaims("acme", TimeUnit.DAYS.toMillis(50), true),
        clock,
        "serialized-abc"
    );

    expect(schedulerService.isTerminated()).andReturn(false).times(10);

    // Create running: submit the standard check every 24 hours
    expectScheduleAtFixedRate(future);
    // 1st check (50 days) does not expire soon, so no additional check
    expect(manager.registerOrValidateLicense(anyString())).andReturn(license).times(10);
    // 2nd check (24 hours remaining) does not expire soon, so no additional check
    // 3rd check (23 hours remaining) schedules next check
    expectSchedule(1, TimeUnit.HOURS);
    // 4th check (1 hour remaining) schedules next check
    expectSchedule(1, TimeUnit.HOURS);
    // 5th check (59 minutes remaining) schedules next check
    expectSchedule(5, TimeUnit.MINUTES);
    // 6th check (5 minutes remaining) schedules next check
    expectSchedule(5, TimeUnit.MINUTES);
    // 7th check (4 minutes remaining) schedules next check
    expectSchedule(15, TimeUnit.SECONDS);
    // 8th check (1 minutes remaining) schedules next check
    expectSchedule(15, TimeUnit.SECONDS);
    // 9th check (0 minutes remaining) schedules next check
    expectSchedule(15, TimeUnit.SECONDS);
    // 10th check (very expired) schedules next check
    expectSchedule(15, TimeUnit.SECONDS);

    replayAll();

    AtomicReference<LicenseManager> cancelled = new AtomicReference<>();
    runningManager = new LicenseManagers.RunningLicenseManager(
        manager,
        schedulerService,
        cancelled::set
    );

    // 1st check (50 days)
    clock.sleep(license.expirationMillis() - TimeUnit.DAYS.toMillis(50));
    assertEquals(50, license.timeRemaining(TimeUnit.DAYS));
    runningManager.checkLicense();

    // 2nd check (24 hours remaining)
    clock.sleep(TimeUnit.DAYS.toMillis(49));
    assertEquals(24, license.timeRemaining(TimeUnit.HOURS));
    runningManager.checkLicense();

    // 3rd check (23 hours remaining)
    clock.sleep(TimeUnit.HOURS.toMillis(1));
    assertEquals(23, license.timeRemaining(TimeUnit.HOURS));
    runningManager.checkLicense();

    // 4th check (1 hour remaining)
    clock.sleep(TimeUnit.HOURS.toMillis(22));
    assertEquals(60, license.timeRemaining(TimeUnit.MINUTES));
    runningManager.checkLicense();

    // 5th check (59 minutes remaining)
    clock.sleep(TimeUnit.MINUTES.toMillis(1));
    assertEquals(59, license.timeRemaining(TimeUnit.MINUTES));
    runningManager.checkLicense();

    // 6th check (5 minutes remaining)
    clock.sleep(TimeUnit.MINUTES.toMillis(59 - 5));
    assertEquals(5, license.timeRemaining(TimeUnit.MINUTES));
    runningManager.checkLicense();

    // 7th check (4 minutes remaining)
    clock.sleep(TimeUnit.MINUTES.toMillis(1));
    assertEquals(4, license.timeRemaining(TimeUnit.MINUTES));
    runningManager.checkLicense();

    // 8th check (1 minutes remaining)
    clock.sleep(TimeUnit.MINUTES.toMillis(3));
    assertEquals(1, license.timeRemaining(TimeUnit.MINUTES));
    runningManager.checkLicense();

    // 9th check (0 minutes remaining)
    clock.sleep(TimeUnit.SECONDS.toMillis(60));
    assertEquals(0, license.timeRemaining(TimeUnit.SECONDS));
    runningManager.checkLicense();

    // 10th check (very expired)
    clock.sleep(TimeUnit.DAYS.toMillis(100));
    assertEquals(-100, license.timeRemaining(TimeUnit.DAYS));
    runningManager.checkLicense();

    verifyAll();
  }


  @Test
  public void shouldSubmitNextLicenseCheckForInvalidLicense() throws InvalidLicenseException {
    final int count = 10;
    // Create running: submit the standard check every 24 hours
    expectScheduleAtFixedRate(future);
    // 1st check fails
    expect(schedulerService.isTerminated()).andReturn(false).times(count);
    expect(manager.registerOrValidateLicense(anyString()))
        .andThrow(new InvalidLicenseException("invalid"))
        .times(count);
    expectSchedule(15, TimeUnit.SECONDS, count);

    replayAll();

    AtomicReference<LicenseManager> cancelled = new AtomicReference<>();
    runningManager = new LicenseManagers.RunningLicenseManager(
        manager,
        schedulerService,
        cancelled::set
    );

    // 1st check
    for (int i = 0; i != count; ++i) {
      runningManager.checkLicense();
    }

    verifyAll();
  }

  @Test
  public void shouldDelegateRunningLicenseManagerMethodsToFuture()
      throws ExecutionException, InterruptedException, TimeoutException {
    // Submit the standard check every 24 hours
    expectScheduleAtFixedRate(future);
    expect(future.compareTo(future)).andReturn(0);
    expect(future.compareTo(future2)).andReturn(1);
    expect(future.isCancelled()).andReturn(false);
    expect(future.isDone()).andReturn(true);
    expect(future.get()).andReturn(null);
    expect(future.get(10, TimeUnit.DAYS)).andReturn("days");
    expect(future.get(1, TimeUnit.DAYS)).andReturn("day");
    expect(future.getDelay(TimeUnit.SECONDS)).andReturn(100L);
    expect(future.cancel(false)).andReturn(false);
    expect(future.cancel(true)).andReturn(true);
    manager.doStop();
    expectLastCall().times(2);

    replayAll();

    AtomicReference<LicenseManager> cancelled = new AtomicReference<>();
    runningManager = new LicenseManagers.RunningLicenseManager(
        manager,
        schedulerService,
        cancelled::set
    );
    assertNull(cancelled.get());

    assertNull(runningManager.get());
    assertEquals("days", runningManager.get(10, TimeUnit.DAYS));
    assertEquals("day", runningManager.get(1, TimeUnit.DAYS));
    assertEquals(100L, runningManager.getDelay(TimeUnit.SECONDS));
    assertFalse(runningManager.isCancelled());
    assertTrue(runningManager.isDone());
    assertFalse(runningManager.cancel(false));
    assertTrue(runningManager.cancel(true));
    assertEquals(1, runningManager.compareTo(future2));
    assertEquals(0, runningManager.compareTo(future));

    verifyAll();
  }

  protected void asserNextLicenseCheck(
      long tillExpires,
      TimeUnit expireUnit,
      long expected,
      TimeUnit expectedUnit
  ) {
    assertEquals(
        expectedUnit.toMillis(expected),
        runningManager.millisForNextLicenseCheck(expireUnit.toMillis(tillExpires))
    );
  }

  protected void expectDoStart(LicenseManager manager) {
    manager.doStart();
    expectLastCall();
  }

  protected void expectDoStop(LicenseManager manager) {
    manager.doStop();
    expectLastCall();
  }

  protected void expectScheduleAtFixedRate(ScheduledFuture<Object> returning) {
    expectScheduleAtFixedRate(1, 1, TimeUnit.DAYS, returning);
  }

  protected void expectScheduleAtFixedRate(
      long initialDelay,
      long period,
      TimeUnit unit,
      ScheduledFuture<Object> returning
  ) {
    schedulerService.scheduleAtFixedRate(
        anyObject(Runnable.class),
        eq(initialDelay),
        eq(period),
        eq(unit)
    );
    expectLastCall().andReturn(returning);
  }

  protected void expectSchedule(
      long period,
      TimeUnit unit
  ) {
    expectSchedule(period, unit, 1);
  }

  protected void expectSchedule(
      long period,
      TimeUnit unit,
      int count
  ) {
    expect(
        schedulerService.schedule(
            anyObject(Runnable.class),
            eq(unit.toMillis(period)),
            eq(TimeUnit.MILLISECONDS)
        )
    ).andReturn(null).times(count);
  }

  protected void expectShutdownNow() {
    expect(
        schedulerService.shutdownNow()
    ).andReturn(Collections.emptyList());
  }

}