# Cocytus over Memcached

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

