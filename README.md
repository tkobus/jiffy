# Jiffy - A Lock-free Skip List with Batch Updates and Snapshots

Jiffy is the first linearizable, lock-free, ordered key-value index (sorted map)
that supports:
- batch updates, i.e., a set of put/remove operations that are executed
  atomically,
- consistent snapshots, which are used by, e.g., range scan operations.

Jiffy is built as a multiversioned lock-free skip list and relies on
system-provided timestamps (e.g., on x86_64 obtained through the Time Stamp
Counter register) to generate version numbers at minimal cost. For faster skip
list traversals and better utilization of CPU caches, key-value entries are
grouped into immutable objects called revisions. By (automatically)
controlling the size of new revisions, our index can adapt to varying
contention levels (e.g., smaller revisions are more suited for write-heavy
workloads). Structure modifications to the index, which result in changing
the size of revisions, happen through (lock-free) skip list node split and
merge operations that are carefully coordinated with the update operations.

The algorithm details and Jiffy's evaluation can be found in a paper titled
*Jiffy: A Lock-free Skip List with Batch Updates and Snapshots* by Tadeusz
Kobus, Maciej Kokociński and Paweł T. Wojciechowski, published at PPoPP '22.

### Contents

The repository contains the source code of Jiffy as well as our test harness
for concurrent data structures. During a test, concurrent threads log traces of
performed operations (the traces also include system-provided timestamps).
Traces are then transformed into a graph that reflects various relationships
between the logged events, e.g., write-read/write-write dependencies, program
and timestamp order, batch update and snapshot-based constraints, etc. We
iteratively refine the graph by inferring new relationships between events and
check whether the graph is still acyclic.

Currently, for legal reasons, we cannot include in the repository a benchmark
suit that we used to conduct performance assessment of Jiffy and its
competitors. However, we to revise the code and publish it as soon as possible.
In case you are interested in rerunning the performance tests from the paper,
contact [the repository owner](http://www.cs.put.poznan.pl/tkobus/contact/contact.html)

### Prerequisites

- Java 11+,
- gradle 7.0.2 (tested also with gradle 6.5),


### Running the correctness tests

The basic test involves running 16 concurrent threads executing a mix of all
available operations (including batch updates, range scans, etc). Various
statistics will be displayed on the screen (a summary for each thread
individually and for all threads together). Then conctest will perform
basic validation of the return values of the operations performed.


1. Run the basic tests (Jiffy's min/max sizes of revisions are set
   to 3/10 so that we generate a lot of revision splits or merges).

   $ gradle runConctest

2. Show test options:

   $ gradle runConctest -Pmyargs="-h"

3. Run full concurrency tests for the above scenario (could take a while
   to complete).

   $ gradle runConctest -Pmyargs="-m 2"

### Disclaimer

This code has been only partially cleaned up, so it still includes a lot of
(disabled) debug.


MIT license, 2021 Poznan University of Technology
