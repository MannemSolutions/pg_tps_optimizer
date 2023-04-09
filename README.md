# pg_tps_optimizer

## General information
This repo is about doing some load testing on Postgres.
The concept is that depending on #clients first tps increases, and then latency increases, as such tps/latency works like a curve.
We want to find the optimal #clients with max utilization (max tps against lowest latency).

Max utilization is system dependant:
* system: number of cpu, memory, storage performance, etc.
* max_connections (but only little impact)
* table, indexes, etc. (which is why we allow to set query and define own strcture and dataset)
* postgres config (which could be tuned by run pg_cpu_load_tester to multiple configs and recording #clients, #tps and #latency)

How it works:
* We generate a fibonacci sequence (1, 2, 3, 5, 8, 11, etc... to max)
* We run 1 #clients and increase according to this sequence
* We check tps, latency and dicide the two
* we wait until it stablizes and then
  * print #clients, #tps, latency(ms) and tps/latency
  * move to the next
* we end with a report describing optimal #clients to run this query with max latency against min latency
