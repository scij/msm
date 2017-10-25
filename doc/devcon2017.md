# Messaging minus middleware - multicast macht's möglich

## Vorgeschichte

DB Netz - Multicast mit Tibrv. Teuer aber gut.
Preis pro Workstation?

Verarbeitung von Echtzeitdaten. Schnelligkeit geht vor Vollständigkeit.
Pipeline
100 Msg/s
Syntherei
200 Msg/s

Prognose
3.000.000 Msg/s
Zur Verteilung an 1.000 Arbeitsplätze

Spitzendurchsatz Tibco EMS: 200 Msg/s
Bei Verzicht auf Exactly Once 1.000 Msg/s

Fehlertoleranz: Kompliziertes Verfahren mit Shared Disk.
Stuck Message Effekt beim Fail-over.

Wenn ich das ersetzen will, brauche ich eine Middleware
ohne Server.

## Marktforschung

ZeroMQ is a serverless low latency middleware.

[Request Reply|https://github.com/imatix/zguide/raw/master/images/fig2.png]

Sieht gut aus. Kein Server.

Geht auch Lastverteilung (mehrere Consumer)?

[Pub Sub|https://github.com/imatix/zguide/raw/master/images/fig4.png]

Super. Und mehrere Producer?

[Push Pull|https://github.com/imatix/zguide/raw/master/images/fig6.png]

Und jetzt kombinieren.

[Extended Pub Sub|https://github.com/imatix/zguide/raw/master/images/fig14.png]

Wait.

How about Multicast? (Tibrv uses Multicast)

Wie funktioniert das eigentlich?

Addressbereich ???
IP, Paketorientiert, Verbindungslos, keine Transportsicherung.
Darauf gibt es verschiedene Protokolle, die das anbieten. Die sind generell NAK-basiert (TCP ist ACK-basiert)

* TRDP von Tibco. Wird von RV genutzt, closed source.
* PGM. Wird von Tibco alternativ unterstützt, wurde von Talarian entwickelt (SmaSo). Wird
von Cisco in Routern unterstützt, um die NACK-Implosion zu verhindern. Nachteil:
Ein eigenes Protokoll, braucht Raw-Sockets, braucht root und auf BSD-ishen OS gibt es keine
Raw-Sockets, da muss man den PF anprogrammieren. Noch ein Nachteil: PGM (und TRDP) können
kein Loopback, d.h. zwei Consumer mit der gleichen Addresse auf demselben Host. Deshalb
gibt es bei Tibco auf jedem Host einen RV-Daemon. Wieder ein Server.
* EPGM kapselt PGM in UDP und braucht kein root. Kann aber trotzdem kein Loopback.
* NORM (Nak Oriented Reliable Multicast) basiert auf RFC 3940 und es gibt eine Referenzimplementierung
vom NRL und deshalb ist es auch richtig klassische Public Domain. NORM wird auch als Transport
für ZeroMQ unterstützt aber rudimentär und nicht sehr stabil.

