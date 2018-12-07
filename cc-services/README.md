# Cloud

Here we store CCloud utilities (written in Go) that are closely tied to Kafka.
Currently we have a Trogdor client, performance tests that leverage Trogdor and Soak cluster clients.

# Soak Testing
### How to run on CPD
1. Configurations
    * `charts/cc-trogdor-service/values.yaml` with the desired docker image, number of Trogdor agents
    * `charts/cc-soak-clients/values.yaml` with the same number of Trogdor agents, bootstrap URL, api keys/secrets, docker image
    * `charts/cc-soak-clients/templates/configMaps.yaml` with the desired throughput/client count for every topic
2. Spawn the Trogdor agents
    * `helm install charts/cc-trogdor-service`
    * Inspect the pods: `kubectl get pods -n default | grep "trogdor"`
3. Spawn the Soak clients 
    1. `helm install charts/cc-soak-clients`
    2. Edit the clients-spawner cronjob to start soon
        `kubectl edit cronjobs -n default clients-spawner`. Change the `schedule:` field with the appropriate cron schedule expression.
    3. Track the new clients-spawner pod from the cronjob: `kubectl get pods --all-namespaces | grep "clients-spawner"`
    
If all goes well, you should have clients producing and consuming from the configured Kafka cluster.

It is easiest to inspect the tasks by manually querying the Trogdor coordinator.
```bash
kubectl port-forward cc-trogdor-service-coordinator-0 9002:9002 &
curl -X GET 'http://localhost:9002/coordinator/tasks?state=RUNNING'
```


### Testing new builds
To test a newer build of `cc-trogdor-service` or `cc-soak-clients`, you need to build a docker image and push it to [JFrog's Artifactory](https://confluent.jfrog.io/confluent/webapp/)

#### cc-trogdor-service
The soak tests can prove useful in cases where you'd like to test changes in Trogdor itself.
To test the new changes, you need to have a docker image of the build. This is easily done through Semaphore:
1. Push your changes to a branch
2. Open https://semaphoreci.com/confluent/ce-kafka and click on the plus (+) sign next to Branches
3. Add your branch and wait for the build to finish
4. When the build passes, open "Job #1"->"make build" and take the docker image tag from there. It should be something like: `Successfully tagged confluentinc/ce-kafka:0.8.0-beta1-5210-g1fd081560
5. Make the `cc-services/trogdor/Dockerfile` dockerfile load that image (`FROM confluentinc/ce-kafka:0.8.0-beta1-5210-g1fd081560`)
6. Build that Dockerfile - `docker build .`
7. The previous command should have output something similar to `Successfully built 7158e4c15226`. Tag that docker build with an arbitrary **low** version - `docker tag 7158e4c15226 confluent-docker.jfrog.io/confluentinc/cc-trogdor:v0.0.0.1`
8. Push that to JFrog - `docker push confluent-docker.jfrog.io/confluentinc/cc-trogdor:v0.0.0.1`
9. Edit the `cc-services/charts/cc-trogdor-service/values.yaml` to use that new `0.0.0.1` version (`tag: 0.0.0.1`)

#### cc-soak-clients
You might also want to test out new builds of the code that spawns the soak clients. This can easily be done locally
1. `cd ./cc-services/soak_cluster`
2. `docker build .`
3. The previous command should have output something similar to `Successfully built 7158e4c15226`. Tag that docker build with an arbitrary **low** version - `docker tag 7158e4c15226 confluent-docker.jfrog.io/confluentinc/cc-soak-clients:v0.0.0.1`
4.  `cc-services/charts/cc-soak-clients/values.yaml` to use that new `0.0.0.1` version (`tag: 0.0.0.1`) 
