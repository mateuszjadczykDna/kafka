# soak_clients

The soak_clients package consists of the following components:
* [clients spawner](./soak_clients/main.go) - Creates Trogdor tasks (via its REST API) that are used for soak testing
* [status_reporter](./soak_clients/status_reporter.go) - Periodically queries the currently running Trogdor tasks and reports their status
* [performance_tests](./performance/main.go) - Creates Trogdor tasks (via its REST API) that are used for performance testing

All of them are callable through the [soak_clients CLI](./main.go) 

### clients spawner
The clients spawning code takes in a JSON definition of the type of tasks we want to run. An example of the specification can be found [here](./soak_clients/config/baseline.json).
We split tasks by two types:
* _long-lived_ - These tasks simply run for `long_lived_task_duration_ms`
* _short-lived_ - These tasks run for `short_lived_task_duration_ms`. The main goal with these is to mimick new clients. The spawner will create tasks for these in such a way that we always have a short-lived task running up to the end of the long-lived tasks. They will be re-scheduled every `short_lived_task_reschedule_delay_ms`.
  * example: if we have a long-lived task scheduled to run 13:00-14:00 (1hr) and a short-lived task duration of 15m, we would have 4 short-lived tasks spawned (13:00-13:15, 13:15-13:30, 13:30-13:45, 13:45-14:00).
  * if we were to configure `short_lived_task_reschedule_delay_ms` to 30 minutes, we would have only two short-lived tasks in the same example period (13:00-13:15, 13:45-14:00)  

We define the amount of tasks on a per-topic basis inside the `topics` field. There we also define the total produce/consume throughput we want the topic to have. Said throughput then gets split evenly across every task. Note that this means you may not always have the desired throughput because the short-lived tasks will not be running during `short_lived_task_reschedule_delay_ms`

### performance tests
[README.md](./performance/README.md)
