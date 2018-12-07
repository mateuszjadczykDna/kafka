This program orchestrates the Trogdor agents that operate on the Soak Cluster.
It has multiple responsibilities:
    
  * Create Trogdor scenarios with realistic workloads that run on the cluster for extended periods of time.
  * Run periodic jobs that inspect the health and status of said workloads

# Usage
To run the soak clients in CPD:

```
cd ../
vi charts/cc-soak-clients/values.yaml # edit bootstrapServer and apiKey/Secret
vi charts/cc-soak-clients/templates/configMaps.yaml # optionally modify topics.json
helm install charts/cc-trogdor-service/  # spawn trogdor coordinator and agents
helm install charts/cc-soak-clients/  # spawn soak clients and cronjob for status reporting
```

# Possible future improvements
* Allow for interactive task start/pause/stoppage
* Support more diverse scenarios
