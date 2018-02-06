# com.senacor.msm.norm

A Clojure library for asynchronous truely server-less communication.

## Description

### What is MSM?

MSM is **M**icro **S**ervice **M**esh, a middleware library for server-less
communication between microservices.
Conventional messaging middleware relies on a central component, a messaging server
which needs to be known to all participants in a communication relationship. Central
servers are a single point of failure, a bottleneck and often overhead.
As you introduce replication and distribution for fault tolerance and scalability you
will learn that the CAP theorem also applies to messaging middleware.

MSM avoids a central server by shifting responsibility for message distribution and
routing down to the IP network layer. Senders distribute messages via IP Multicast to
receivers they do not have to know.
The use of IP Multicast leads to a connection-less communication (i.e. no TCP/IP) and
requires special protocols for transmission- and flow-control. The most popular protocols
in this domain are PGM (and it's sister protocol EPGM) and NORM. PGM is an IP protocol and
requires root privileges to communicate. EPGM encapsulates traffic in UDP. Both are unable
to send and receive messages on the same host (loopback communication) which makes service
allocation tricky. This is why I chose an alternative protocol: NORM

### What is NORM?

NORM means **N**ACK **O**riented **R**eliable **M**ulticast, a UDP-based multicast
protocol with transport guarantees and bandwidth management.

NORM is specified in [RFC 5740](https://tools.ietf.org/html/rfc5740) and
[RFC 5401](https://tools.ietf.org/html/rfc5401). A
public domain (in the historical sense of the word) implementation 
by the Naval Research Lab is available at [the NRL web site](https://www.nrl.navy.mil/itd/ncs/products/norm)

### What is clojure.core.async?

Clojure.core.async is a library extending clojure to provide asynchronous
programming in a functional way developed by Rich Hickey and the c.c.a-Team.
Details can be found at [The clojure.core.async github page](https://github.com/clojure/core.async).

## Usage

    (let [event-chan-out (chan 5)
          event-chan-in (mult event-chan-out)
          sync-chan (chan)
          instance (ctl/init-norm event-chan-out)]
      (let [session (ctl/start-norm-session instance "239.192.0.1" 7100 1 :loopback true)]
        (mon/mon-event-loop event-chan-in)
        (let [out-chan (chan)
              sndr (snd/create-sender session 0 event-chan out-chan sync-chan 128)]
          (>!! out-chan (msg/Message->bytes (msg/create-message "DEMO.COUNT" "Message")))
          (close! out-chan))))
        

## Related work

My search on server-less middleware returned two working open source projects: ZeroMQ and NanoMsg (the latter
being a fork of the former). Both promise server-less communication but as I looked closer I found out
that this is not implemented to the full extend. ZeroMQ needs a broker for n:m communication
re-introducing the server. In addition the multicast support is based upon PGM and EPGM which both
do not support local communication on a single server. There is NORM support in ZeroMQ but it appears
to be broken. NanoMsg has the same troubles and lacks multicast support.

## License

Copyright © 2017 Jürgen Schiewe

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
