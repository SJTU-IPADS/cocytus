# About Cocytus

Cocytus is a distributed in-memory key-value store which achieves both efficiency and availability by using hybrid erasure coding and replication. With low overhead for latency and throughput, it saves up to 46% memory compared to traditional primary-backup replications when tolerating two failures. An online recovery scheme is applied when failures are detected so that the whole system is able to serve key-value requests continuously. 

If you use Cocytus in your work or research, please kindly let us know. We also encourage you to reference our paper:

Here is the bibtex:

    @inproceedings{zhang2016cocytus,
        author = {Zhang, Heng and Dong, Mingkai Dong and Chen, Haibo},
        title = { Efficient and Available In-memory KV-Store with Hybrid Erasure Coding and Replication},
        booktitle = {Proceedings of the 14th USENIX Conference on File and Storage Technologies},
        series = {FAST '16},
        year = {2016},
        location = {Santa Clara, CA},
        publisher = {ACM},
    } 


## Dependencies

* libevent, http://www.monkey.org/~provos/libevent/ (libevent-dev)
* Jerasure & GF-Complete, http://jerasure.org/

## Build

To configure and build Cocytus, you need to install the required
dependencies first. Then execute the following commands.
```
$ ./autogen.sh
$ ./configure --enable-cocytus
$ make
```

If the libraries are not installed in the default location, you may
need to add the CPPFLAGS and LDFLAGS explicitly in the configuration
phase.

```
$ ./autogen.sh
$ ./configure --enable-cocytus CPPFLAGS='-I/path/to/include' LDFLAGS='-L/path/to/libs'
$ make
```

## Website

* http://ipads.se.sjtu.edu.cn/pub/projects/cocytus



