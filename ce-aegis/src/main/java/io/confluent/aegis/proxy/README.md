Aegis
=====
Aegis is a Kafka proxy which sits between the clients and the brokers.  It
accepts connections from Kafka clients, which we call "tenants."  It forwards
these connections to Kafka brokers.

Each broker in the Kafka cluster has its own "endpoint" on Aegis.  We do not
forward requests intended for one broker to a different broker.  So, for
example, if the cluster has 10 brokers, Aegis will listen on 10 separate ports.

   Tenant  Aegis   Broker
             |
      -----> |
             |------>
             |<------
      <----- |
             |

Networking
----------
Aegis uses the (Netty)[https://netty.io/] framework for networking.

There is a separate thread pool for each endpoint.  This thread pool handles
both tenant-side and broker-side connections, in order to avoid context
switching.
