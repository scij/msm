# MSM Backlog

## Stateful queues einbauen.

Analog zu Tibco FT Groups.
Jeder Receiver empfängt alle einlaufenden Nachrichten. Sie werden aber nur
insoweit verarbeitet, wie dies ohne Seiteneffekte möglich ist. Nur der aktive
Prozess darf auch Seiteneffekte erzeugen. Wenn der ausfällt, übernimmt ein
anderer Prozess.

### Lösungsansatz

Auch hier machen wir eine Out-of-band-Signalisierung über commands. Jeder
Teilnehmer kennt alle anderen Teilnehmer. Wenn der aktive ausfällt, wird
der erste Nachrücker der aktive Teilnehmer (analog zu stateless ist die
Sortierung der Receiver für alle Clients gleich).
Wie beim stateless werden alle Nachrichten seit dem letzten OK nachverarbeitet.

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

Vergleich JMS (verschiedene Implementierungen und Konfigurationen) mit MSM.
Besonders interessant ist ein Szenario mit vielen Subscribern.

## Docker

* Funktioniert das überhaupt auf Docker? 
* Performance messen. 
* Generierung der Docker-Images im Build.