# Stateless Sessions

## The requirement

We would like to have multiple senders and receivers and each message
should be consumed by exactly one receiver. This pattern is well suited
for stateless receivers and would allow parallel processing.
JMS has a concept of queues which does exactly this - only that it
involves middleware and centrally managed ressources.

## Solution approaches

The middleware work we did so far delivers messages to all receivers.
We now need to make sure that exactly one receiver picks up and processes
a message.

### Sender controlled dispatch

Tibco Rendezvous seems to work sender controlled when *Distributed Queues*
aka DQs are used. The sender is aware of all the consumers for a
particular subject and even how quickly they process incoming messages.
It can now load-balance messages by assigning each message to one
receiver. Since receivers have to confirm successful processing of a 
message back to the sender reassignment and retransmission is also an
option for the sender.

### Sharding

Sharding has become popular with distributed databases. Each item
in the database has a sharding key and each node in the distributed
database handles a particular subset of the sharding keys, usually by
computing some sort of hash over the sharding key.
In a messaging world each message would carry a sharding key and
each receiver would check if the hash over the sharding key matches its
key responsibility. This scenario works without sender involvment aside
from the provision of a sharding key in the message.
Receivers however would have to know how many receivers there are
and they would have to agree on sharding (who processes which 
shard key).
Load balanced dynamic sharding would be possible but avoiding message loss
in case of receiver nodes leaving the system unexpectedly
will be a challenge.

## Design decision

I decided to implement the Sharding approach because it is a lot simpler
to implement, it requires less messaging and fully decouples the sender
from the receivers. Also note that with *sender controlled dispatch* the
senders need to coordinate load balancing with each other.

### Sharding key and sharding

The sender puts a message sequence number (a 64 bit integer) into
the static part of the message header where it can quickly be parsed.
Receivers simply take the *sequence number* MOD the *number of receivers*
and compare the result to their *receiver index*.

### Coordinating receivers

Receivers now need to find our how many there are and they need to
agree on receiver indexes. Each receiver periodically sends out an
*I am alive* message with its own ID and the message label it is
listening to. 

Each receiver maintains a sorted map (sorted my receiver ID) of
all receivers along with an expiry timestamp. Sorting makes sure
that all receivers agree on the *receiver index*.

Now timing becomes an issue. A joining receiver will be introduced
to each of the existing receivers at a different point in time but
updates to the receiver table must be synchronized or message will
processed by more than one receivers or not at all.

Therefore the receiver table is versioned and because we do not
trust timing we use message sequence numbers as our heartbeat:
A joining receiver will announce it will be joining with a  given
future sequence number and all receivers will prepare a new
receiver table which will be activated once this new sequence number
arrives.
