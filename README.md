# stacktainer-kafka

A throwaway single-node Kafka broker for simulated ESS data. Streams live here only
until the file-writer consumes them -- normally a few seconds -- so the server is
configured to **discard data promptly** and to keep it **in RAM** rather than on disk.

# Build

Use the Confluent Kafka Debian packages to build an Apptainer image which will launch a standalone
server listening on `localhost:9092` with suitable defaults.

```cmd
apptainer build stacktainer-kafka.sif image.def
```

# Run
The Kafka server can be started through either `run`, in which case the server logging output is visible,
or in a detached thread via `instance run` in which case no output is visible and the service must
be managed through `apptainer instance`

```cmd
apptainer run --writable-tmpfs stacktainer-kafka.sif 
```

```cmd
apptainer instance run --writable-tmpfs stacktainer-kafka.sif 
```

The writable temporary filesystem overlay is kept for the benefit of anything in the
image that expects to write to its own root, but the broker no longer depends on it:
log segments, the generated `server.properties` and the server's own log4j output all
go to `${KAFKA_STORAGE_DIR}` (see below).

Also note that the Kafka KRaft server uses port `9093`, so either launch method is liable to fail with
a hard-to-spot error message if either port `9092` or `9093` is already in use.

To support running on user-specified ports, the image uses two environment variables which default to
`BROKER_PORT=9092` and `CONTROLLER_PORT=9093`, and that can be overridden in the launch, e.g.,

```cmd
apptainer run --env BROKER_PORT=19092 --env CONTROLLER_PORT=19093 --writable-tmpfs stacktainer-kafka.sif
```

# Storage and retention

## Where the data goes

`KAFKA_STORAGE_DIR` defaults to `/dev/shm/kafka-${BROKER_PORT}`. `/dev/shm` is a tmpfs
on every Linux host and Apptainer bind-mounts the host `/dev` by default, so the log
directory is RAM-backed with no extra launch flags and no `apptainer.conf` changes.
The path is keyed by broker port so two instances on one host cannot collide.

The default tmpfs size is half of host RAM, which is ample on a simulation VM. Check
with `df -h /dev/shm` -- containerised or memory-capped hosts sometimes ship a 64 MiB
`/dev/shm`, which is too small. The broker prints the directory and its free space at
startup, and warns if the filesystem cannot hold even one busy partition.

The startscript removes `${KAFKA_STORAGE_DIR}/data` and `${KAFKA_STORAGE_DIR}/run` both
before formatting and after the broker exits, so nothing survives a restart and stopping
the instance gives the memory back. (Only those two subdirectories are touched, never
`KAFKA_STORAGE_DIR` itself.)

To put the logs back on disk -- for a host with little RAM, or to inspect segments after
a crash -- point the variable somewhere else:

```cmd
apptainer instance run --env KAFKA_STORAGE_DIR=/var/tmp/kafka --writable-tmpfs stacktainer-kafka.sif
```

## How long data is kept

| variable | default | meaning |
|----------|---------|---------|
| `KAFKA_RETENTION_MS`    | `300000` (5 min)     | delete records older than this |
| `KAFKA_RETENTION_BYTES` | `268435456` (256 MiB) | hard ceiling per partition |
| `KAFKA_SEGMENT_MS`      | `30000` (30 s)       | how often the active segment is closed |

```cmd
apptainer instance run --env KAFKA_RETENTION_MS=60000 --writable-tmpfs stacktainer-kafka.sif
```

Worst case a single partition occupies `KAFKA_RETENTION_BYTES` plus one active segment
(`log.segment.bytes`, 128 MiB), so roughly 384 MiB per partition if a producer sustainedly
outruns the file-writer. In normal use the retention window dominates and usage is just
`data rate x KAFKA_RETENTION_MS`.

`KAFKA_SEGMENT_MS` matters more than it looks: **Kafka never deletes the segment it is
currently appending to**, whatever the retention settings say. The stock configuration
this image used to ship rolled a new segment only after 1 GiB or a week, so a low-rate
topic kept everything in one open segment and retention never applied -- which is how
`/tmp` on the simulation VM filled up. Closing the segment every 30 s is what makes the
retention window real. Idle topics do not roll empty segments, so this costs nothing
when no simulation is running.

## Choosing a retention window

Five minutes is roughly two orders of magnitude more slack than the orchestrated
workflow needs: `mp-nexus-splitrun` starts the file-writer job *before* the simulation
and the writer consumes continuously, so it is never more than seconds behind.

Raise `KAFKA_RETENTION_MS` if you drive the pipeline by hand and start a write job with
a start time in the past (e.g. via `mp-writer-from`), since anything older than the
window is already gone. Note also that `mp-writer-list` can only report jobs whose
status messages are still within the window.

Per-topic overrides set at topic creation time (`mccode_plumber.kafka.register_kafka_topics`)
still take precedence over these broker-wide defaults.

## Via the `stacktainer` modulefile

`start-kafka` forwards only `--broker`/`--controller`, so pass the storage and retention
variables through Apptainer's environment prefix instead:

```cmd
module load stacktainer/1.1
export APPTAINERENV_KAFKA_RETENTION_MS=60000
start-kafka
```
