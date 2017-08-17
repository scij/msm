# MSM Backlog

## Queues einbauen

Bis jetzt verhält sich MSM immer wie ein Topic. Jede Nachricht geht an alle
Receiver, die sich auf das Label angemeldet haben.

Mit Queues wird jede Nachricht nur noch von einem Receiver verarbeitet. Das
soll aber dynamisch sein, d.h. ich will neue Receiver einbringen oder bestehende
stoppen können, ohne das ich das System neu konfigurieren muss.
Ich will auch den (oder die) Sender nicht damit belasten.

Lösungsansatz:

Sharding. Die Receiver kennen einander und teilen sich die Nachrichten nach
einem Sharding-Schema auf. Jetzt müssen wir nur noch das Problem der
dynamischen Veränderung lösen. Wenn ein Receiver aussteigt, sollen die anderen
das merken und das Sharding neu ausbalancieren.
Das sichern wir schlauerweise über eine out-of-band-Signalisierung ab,
mit der jeder Receiver sagt, dass er noch lebt und welche Nachricht er zuletzt
verarbeitet hat. Wenn jetzt ein Receiver aussteigt, wird die Verteilung von
jedem Receiver neu berechnet und jeder Receiver arbeitet die nach dem letzten
Count aufgelaufenen Nachrichten nach soweit er dafür zuständig ist.

## Stateful queues einbauen.

Analog zu Tibco FT Groups.

## Router bauen

Multicast endet üblicherweise an der Netzwerkgrenze. Wenn ich Daten zwischen
verschiedenen Netzen austauschen will, muss ich einen Router bauen, der sie 
weiterleitet. Der sollte sich möglichst dynamisch konfigurieren (BGP?).

## Visualisierung

Ich möchte den Traffic überwachen können.
* Welche Nachrichten werden auf welchem Label übertragen
* Wie viele Nachrichten pro Label

## Beispielanwendung

### Replay Zugstandorte.
* Ein Prozess pro BZ-Telegrammdatei
* Prozesse zur Verarbeitung (zum Beispiel Anreicherung um Geodaten)
* Client zur Visualisierung

## Benchmark

Vergleich JMS (verschiedene Implementierungen und Konfigurationen) mit MSM