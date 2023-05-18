# pg_tps_optimizer

## TL/DR

### Binary

If you want to download the binary and run the tool directly:
```
curl -OL https://github.com/MannemSolutions/pg_tps_optimizer/releases/download/v0.1.3/pg_tps_optimizer_v0.1.3_x86_64-unknown-linux-musl.zip
unzip pg_tps_optimizer*.zip
./pg_tps_optimizer --dsn 'host=server1,user=postgres,dbname=postgres,password=password123' --max-wait 10s --min-samples 10 --range 200 --spread 10
```
**Note** that with the binary you need to set all the arguments as shown in the above example...

### Container

If you wanna use the container instead:
```
docker run -e PGHOST=server1,PGUSER=postgres,PGDATABASE=postgres,PGPASSWORD=password123 mannemsolutions/pg_tps_optimizer
```
**Note** that with the container you can set environment variables and leave other arguments default.
If you wanna change defaults, you do need to set all of them (might be fixed in future releases)...

### Arguments

If you wanna change options, you can:
- set `--max-wait` to set the timeout for a step
- set `--min-samples` to wait more timeslices of 200ms before accepting a step to be 'stable',
  printing results and moving to the next number of clients
- set `--range` to change the upper bound.
  The tool follows the fibonacci sequence, so (as an example) there is no difference between an upper bound of 99 and an upper bound of 100.
  In both cases 89 is the last step...
- set `--spread` to be more precise in when the tool decides a step is considered 'stable'.

Example:
```
docker run -e PGHOST=server1,PGUSER=postgres,PGDATABASE=postgres,PGPASSWORD=password123 mannemsolutions/pg_tps_optimizer \
  --max-wait 20s --min-samples 20 --range 200 --spread 5
```
This will do the following:
- wait 20 seconds before a step is timed out
- wait 20 samples (4 seconds) before checking if TPS and latency are stabilized
- not stop at 89, but at 144 clients (next step is 233, which is beyond 200)
- take TPS and latency for stable if they have a standard deviation of 5% max


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
* We run workload with one client and check tps, latency
* we wait until they stabilize and then we print #clients, #tps, latency(ms) and tps/latency
* We increase the number of clients according to fibonacci (2, 3, 5, 8, etc) and repeat the process

In the end you have a report (table) describing TPS and Latency depending on the number of clients.

## How can you leverage this

Depending on your needs you can:
- deduct the number of clients that can do the number of TPS as required
- deduct the number of clients that still has a limited impact on latency (and with that also impact on the rest of the infrastructure)
- deduct the optimal middel ground of the two
- find an optimal starting point for running pg_bench
- compare these stats across multiple arcchitectures
- track these stats as you are applying changes to your setup

## Why another tool next to pg_becnch and other existing tools

Most tools are typically designed to be used as a benchmark investigation.
Properly benchmarking is more of an academic approach and requires a lot of preparation, human effort, hardware resources, etc.
And there are many situations where the investment is too big and the results are not entirely what you are looking for.
Some examples are:
- you wanna run a benchmark, but don;t know the optimal number of clients
- you wanna tune the number of connections in the connection pool for optimal performance
- you wanna know if increasing the number of clients could be benficcial to fixing a performance issue without impacting the production workload
  and as such you wanna know the performance responce of the architecture
- you wanna track performance as you tune configuration, apply environmental changes, etc. and don't want to invest the effort in doing constant full benchmarks
- you want a more broad perspective on the responsiveness of the architecture (instead of, or in addition to) a specific benchmark

In all of these cases pg_tps_optimizer can help you to gather the information.
And of coarse, you can always check by running one pgbench run, just to see if they agree.

## License
We love ur software and want them to grow. Therefore we embrace open Source, and invite you to join the community and contribute.
Therefore we feel that GPL-3.0 license best meets the needs of our users and the community.
In general:
- feel free to use, distribute and even change the code
- if you wanna distribute changed versions we wuld appreciate if yu also upstream your changes so we can expand this project to be even more awesome
Thank you...

## Contributing
We are open source, and are always open to contributions.
- If you experience issues, please submit a github [issue](https://github.com/MannemSolutions/pg_tps_optimizer/issues).
- If you wanna expand features, or fix bugs, please submit a github [Pull Request](https://github.com/MannemSolutions/pg_tps_optimizer/pulls).
