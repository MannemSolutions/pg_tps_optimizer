# pg_tps_optimizer

## General information
The basic idea is to run a benchmark against PostgreSQL where clients run a query as much as possible and latency and transactions per second are tracked.
Once TPS and latency stabilicy, the result is reported and the number of clients is increased.
the number of clients follows teh fibonacci secuence, just so that the increase in number of clients scales with the number of clients.
