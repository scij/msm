(ns com.senacor.msm.core.norm-api
  (:require [clojure.string :as str])
  (:import (mil.navy.nrl.norm NormInstance NormSession NormData NormObject NormNode NormStream)
           (java.nio ByteBuffer)
           (mil.navy.nrl.norm.enums NormEventType NormProbingMode NormAckingStatus NormSyncPolicy NormNackingMode NormRepairBoundary NormObjectType NormFlushMode)
           (java.net InetSocketAddress)))

;;
;; Clojure Binding for NORM
;; This is a low level binding that avoids the Java interop syntax but does little
;; to clojurize the API. This is left to high level api namespaces.
;;

;;
;; Instance
;;

(defn ^NormInstance create-instance
  "Creates a NORM protocol engine. One instance is usually sufficient for an entire
  process. Check the DeveloperGuide for cases when multiple instances are appropriate"
  []
  (NormInstance. false))

(defn destroy-instance
  "Destroys the NORM protocol engine. This will shutdown all communication via NORM."
  [^NormInstance handle]
  (.destroyInstance handle))

(defn ^NormInstance stop-instance
  "Stops the NORM protocol engine. All communication via NORM is suspended but
  can be restarted using restart-instance"
  [^NormInstance handle]
  (.stopInstance handle)
  handle)

(defn ^NormInstance restart-instance
  "Restarts a previously stopped NORM protocol engine"
  [^NormInstance handle]
  (.restartInstance handle)
  handle)

(defn ^Boolean next-event?
  "Checks to see if there is a pending NORM event. NORM uses events to
  asynchronously update the application about status changes of the NORM engine.
  This is a blocking call. A wait time of 0 will return immediately.
  handle the NORM instance
  sec wait time in seconds
  usec wait time millisecond part"
  [^NormInstance handle sec usec]
  (.hasNextEvent handle sec usec))

(def event-type->keyword-map {
                              NormEventType/NORM_TX_QUEUE_VACANCY       :tx-queue-vacancy,
                              NormEventType/NORM_TX_QUEUE_EMPTY         :tx-queue-empty,
                              NormEventType/NORM_TX_FLUSH_COMPLETED     :tx-flush-completed,
                              NormEventType/NORM_TX_WATERMARK_COMPLETED :tx-watermark-completed,
                              NormEventType/NORM_TX_OBJECT_SENT         :tx-object-sent,
                              NormEventType/NORM_TX_OBJECT_PURGED       :tx-object-purged,
                              NormEventType/NORM_TX_CMD_SENT            :tx-cmd-sent,
                              NormEventType/NORM_TX_RATE_CHANGED        :tx-rate-changed,
                              NormEventType/NORM_LOCAL_SENDER_CLOSED    :local-sender-closed,
                              NormEventType/NORM_CC_ACTIVE              :cc-active,
                              NormEventType/NORM_CC_INACTIVE            :cc-inactive,
                              NormEventType/NORM_REMOTE_SENDER_NEW      :remote-sender-new,
                              NormEventType/NORM_REMOTE_SENDER_ACTIVE   :remote-sender-active,
                              NormEventType/NORM_REMOTE_SENDER_INACTIVE :remote-sender-inactive,
                              NormEventType/NORM_REMOTE_SENDER_PURGED   :remote-sender-purged,
                              NormEventType/NORM_REMOTE_SENDER_RESET    :remote-sender-reset,
                              NormEventType/NORM_REMOTE_SENDER_ADDRESS  :remote-sender-address,
                              NormEventType/NORM_RX_OBJECT_NEW          :rx-object-new,
                              NormEventType/NORM_RX_OBJECT_INFO         :rx-object-info,
                              NormEventType/NORM_RX_OBJECT_UPDATED      :rx-object-updated,
                              NormEventType/NORM_RX_OBJECT_COMPLETED    :rx-object-completed,
                              NormEventType/NORM_RX_OBJECT_ABORTED      :rx-object-aborted,
                              NormEventType/NORM_RX_ACK_REQUEST         :rx-ack-request,
                              NormEventType/NORM_RX_CMD_NEW             :rx-object-cmd-new,
                              NormEventType/NORM_GRTT_UPDATED           :grtt-updated,
                              NormEventType/NORM_ACKING_NODE_NEW        :acking-node-new,
                              NormEventType/NORM_SEND_ERROR             :send-error,
                              NormEventType/NORM_EVENT_INVALID          :event-invalid})


(defn next-event
  "Returns the next event. This is a blocking call which will not return
  before a new event is available"
  [^NormInstance handle]
  (let [event (.getNextEvent handle)]
    {:event-type (get event-type->keyword-map (.getType event) :rx-event-invalid),
     :session (.getSession event),
     :node (.getNode event),
     :object (.getObject event)}))

(declare get-node-name)

(defn event->str
  [event]
  (str/join " "
            ["Event"
             (or (:event-type event) "")
             (or (:session event) "")
             (or (get-node-name (:node event)) "")
             (or (:object event) "")]))

;;
;; Session
;;

(defn ^NormSession create-session
  "Creates a new session for NORM communication.
  handle NormInstance handle obtained via create-instance.
  address Network address (multicast or physical).
  port Network port number.
  local-node-id a unique id to distinguish this session from all other
  sessions on the same bus."
  [^NormInstance handle ^String address ^Integer port ^Long local-node-id]
  (.createSession handle address port local-node-id))

(defn destroy-session
  "Destroys the specified session."
  [^NormSession handle]
  (.destroySession handle))

(defn ^NormSession set-tx-port
  "Sets the port to be used for sending data. This function can be used
  to force bind the sending port. Otherwise NORM will choose an unused port.
  The same port could be used for sending and receiving (as set in create-session
  although this is not recommended for performance reasons."
  [^NormSession handle ^Integer port]
  (.setTxPort handle port)
  handle)

(defn ^NormSession set-tx-only
  "Limits the capabilities of this session to NORM sender mode only. The session
  will only open a single sender socket and will not open or bind a receive socket.
  This also implies that the caller must explitely enable multicast feedback"
  [^NormSession handle ^Boolean tx-only]
  (.setTxOnly handle tx-only)
  handle)

(defn ^NormSession set-rx-port-reuse
  "Controls port reuse of the session's receive socket"
  ([^NormSession session ^Boolean reuse]
   (.setRxPortReuse session reuse)
   session)
  ([^NormSession session ^Boolean reuse ^String rx-bind-address ^String sender-address ^Integer sender-port]
   (.setRxPortReuse session reuse rx-bind-address sender-address sender-port)
    session))

(defn ^NormSession set-multicast-interface
  "Binds this session to the specified network interface.
  handle is the NORM session handle.
  if-name is the platform specific name of the network interface like
  e.g. eth0 on a Linux server or en0 on BSD or the network address of
  the interface on a Windows system"
  [^NormSession handle ^String if-name]
  (.setMulticastInterface handle if-name)
  handle)

(defn ^Boolean set-ssm
  "Sets the address for Sender Specific Multicast. Returns true if the
  address could be successfully set and false otherwise."
  [^NormSession handle ^String source-address]
  (.setSSM handle source-address))

(defn ^Boolean set-ttl
  "Sets the value of the time to live (ttl) field of the UDP packets.
  Return true if the value could be set and false otherwise."
  [^NormSession handle ^Byte ttl]
  (.setTTL handle ttl))

(defn ^Boolean set-tos
  "Sets the type-of-service field in the IP multicast packets generated by NORM."
  [^NormSession handle ^Byte tos]
  (.setTOS handle tos))

(defn ^NormSession set-loopback
  "Allows loopback operation for the specified session handle. This will
  cause the session to read back it's own network traffic. While this is
  properly handled by NORM it is not recommended in production because of
  the performance impact. Activating loopback in dev/test environments
  makes sense."
  [^NormSession handle ^Boolean active]
  (.setLoopback handle active)
  handle)

(defn ^Long get-local-node-id
  "Returns the local node ID set by create-session"
  [^NormSession handle]
  (.getLocalNodeId handle))

;;
;; Sender
;;

(defn start-sender
  "Starts the process participation as a sender. All protocol setup
  at the session level needs to be completed before this function is called.
  handle is the NORM session handle.
  session-id an application defined unique identifier for this session that
  helps receivers distinguish different senders.
  buffer-space defines the maximum size (in bytes) used for buffers in FEC.
  segment-size maximum size of payload data excluding protocol and NORM headers.
  block-size is the number of source symbol segments per coding block for FEC.
  num-parity is the number of parity symbol segemnts with block-size + num-parity
  <= 255."
  [^NormSession handle
   ^Long session-id
   ^Long buffer-space
   ^Integer segment-size
   ^Short block-size
   ^Short num-parity]
  (.startSender handle (.intValue session-id) buffer-space segment-size block-size num-parity))

(defn stop-sender
  "Stops the sender and de-allocates all associated resources."
  [^NormSession handle]
  (.stopSender handle))

(defn ^Double get-tx-rate
  "Returns the transmission rate of this sender in bits per second. When
  congestion control is active this value reflects the actual rate at which
  the sender is able to transmit, otherwise it is the value set by set-tx-rate."
  [^NormSession handle]
  (.getTxRate handle))

(defn set-tx-rate
  "Sets the transmission rate of this sender in bits per second. For sessions
  with congestion control activted this will set the initial transmission rate
  but CC will quickly adapt to measured network capacity. For fixed rate sessions
  this function sets the maximum transmission rate"
  [^NormSession handle ^Double rate]
  (.setTxRate handle rate)
  handle)

(defn set-flow-control
  "Sets the scaling factor for timer based flow control in this session. A
  flow control factor of zero disables flow control. Higher flow control factors
  need to be accompanied by larger transmit buffers and cache bounds to maintain
  the state required for buffering. The flow control factor us used to compute
  the time for how long the data is kept on the sender side.

  delayTime = flow-control-factor * GRTT * (backoff-factor + 1)"
  [^NormSession handle ^Double flow-control-factor]
  (.setFlowControl handle flow-control-factor)
  handle)

(defn ^Boolean set-tx-socket-buffer
  "Sets the UDP packet buffer size to a non-standard value. Large buffers
  may help to manage high outbound data rates.
  handle Session handle. Returns true if the buffer could be set successfully
  and false otherwise.
  buffer-size Buffer size in bytes."
  [^NormSession handle ^Long buffer-size]
  (.setTxSocketBuffer handle buffer-size))

(defn set-congestion-control
  "Enables or disables congestion control for this session. The adjust-rate
  parameter controls if rate adjustments computed by CC become effective or if
  they are simply reported via events but are not implemented."
  ([^NormSession handle ^Boolean enable ^Boolean adjust-rate]
   (.setCongestionControl handle enable adjust-rate)
    handle)
  ([^NormSession handle ^Boolean enable]
   (.setCongestionControl handle enable true)
    handle))


(defn ^Boolean set-tx-rate-bounds
  "Sets the lower and upper bound for transmission rates. The NORM
  engine will manage traffic such that the actual transmission rates
  are with in the defined bounds. A negative rate can be used for both
  rates and effectively disables the respective transmission rate bound.
  Return true if the rate change was accepted and false if not.
  min-rate minimal transmission rate in bps.
  max-rate max transmission rate in bps."
  [^NormSession handle ^Double min-rate ^Double max-rate]
  (.setTxRateBounds handle min-rate max-rate))

(defn ^NormSession set-tx-cache-bounds
  "Sets the size of the buffer for unsent objects and the number
  of objects that can be queued for transmission
  handle Session handle
  size-max Number of bytes reserved to store unsent objects
  count-min Minimum number of objects accepted for queuing
  count-max Maximum number of objects accepted for queueing."
  [^NormSession handle ^Long size-max ^Long count-min ^Long count-max]
  (.setTxCacheBounds handle size-max count-min count-max)
  handle)

(defn ^NormSession set-auto-parity
  "Sets the quantity of auto parity data sent at the end of each FEC block. The
  default (zero) will send FEC content only in response to repair requests. The
  value of auto-parity must be less or equal to the num-parity parameter of the
  start-sender call."
  [^NormSession handle ^Byte auto-parity]
  (.setAutoParity handle auto-parity)
  handle)

(defn ^Double get-grtt-estimate
  "Returns the sender's estimate for group round trip time in seconds."
  [^NormSession handle]
  (.getGrttEstimate handle))

(defn ^NormSession set-grtt-estimate
  "Sets the sender's estimate for group round trip time in seconds. This function
  must be called before start-sender is called and can be used to provide an initial estimate
  when the default (0.5s) is inappropriate."
  [^NormSession handle ^Double grtt-estimate]
  (.setGrttEstimate handle grtt-estimate)
  handle)

(defn ^NormSession set-grtt-max
  "Sets the sessions maximum group round trip time."
  [^NormSession handle ^Double grtt-max]
  (.setGrttMax handle grtt-max)
  handle)

(defn ^NormSession set-grtt-probing-mode
  "Sets the probing mode to measure group round trip times. Supported values are
  :active (default and mandatory for congestion control) does a full handshake with
  all receivers to measure the round trip time.
  :passive causes receivers to piggypack the feedback on NACK packets.
  :none disables grtt measuring and avoids the overhead and the benefits"
  [^NormSession handle probing-mode]
  (.setGrttProbingMode handle (case probing-mode
                                :active NormProbingMode/NORM_PROBE_ACTIVE
                                :passive NormProbingMode/NORM_PROBE_PASSIVE
                                :none NormProbingMode/NORM_PROBE_NONE))
  handle)


(defn ^NormSession set-grtt-probing-interval
  "Sets the interval in which the sender sends probing messages to all receivers.
  The sender starts at interval-min and increases the interval up to interval-max.
  Times are specified in seconds
  Default values are 1.0 and 30.0 s"
  [^NormSession handle ^Double interval-min ^Double interval-max]
  (.setGrttProbingInterval handle interval-min interval-max)
  handle)

(defn ^NormSession set-backoff-factor
  "Sets the backoff factor in seconds which impacts various timings in the
  repair process."
  [^NormSession handle ^Double backoff-factor]
  (.setBackoffFactor handle backoff-factor)
  handle)

(defn ^NormSession set-group-size
  "Sets the sender's estimate of the receiver group size"
  [^NormSession handle ^Long group-size]
  (.setGroupSize handle group-size)
  handle)

(defn ^NormSession set-tx-robust-factor
  "Sets the robustness factor which drives the number of times flow control
  packets are repeated. The default value is 20."
  [^NormSession handle ^Integer tx-robust-factor]
  (.setTxRobustFactor handle tx-robust-factor)
  handle)

(defn ^NormData enqueue-date
  "Hand a byte array to NORM for transmission."
  ([^NormSession handle ^bytes data ^Integer offset ^Integer length]
   (.dataEnqueue handle (ByteBuffer/wrap data offset length) 0 length))
  ([^NormSession handle ^bytes data ^Integer offset ^Integer length
    ^bytes info ^Integer info-offset ^Integer info-length]
   (.dataEnqueue handle (ByteBuffer/wrap data offset length) 0 length info info-offset info-length))
  ([^NormSession handle ^String str]
   (.dataEnqueue handle (ByteBuffer/wrap (.getBytes str) 0 (count str)) 0 (count str))))


(defn ^Boolean requeue-object
  "Re-queues the specified object for retransmission. A return value of true
  indicates that the object was queued (not delivered) successfully while false is
  returned if the object as been purged from the cache."
  [^NormSession handle ^NormObject object]
  (.requeueObject handle object))

(defn ^NormStream open-stream
  "Opens and returns a Stream on the specified session. The buffer-size specifies
  the amount of backoff-store allocated to re-transmit data a receiver may have missed.
  Info can be used to transmit meta-data along with the payload."
  ([^NormSession handle ^Integer buffer-size]
   (.streamOpen handle buffer-size))
  ([^NormSession handle ^Integer buffer-size ^bytes info ^Integer info-len]
   (.streamOpen handle buffer-size info 0 info-len))
  ([^NormSession handle ^Integer buffer-size ^bytes info ^Integer offset ^Integer info-len]
   (.streamOpen handle buffer-size info offset info-len)))

(defn close-stream
  "Closes the stream releasing all associated with it. The graceful parameter
  causes the sender to notify all receivers that the stream is closed"
  ([^NormStream handle ^Boolean graceful]
   (.close handle graceful))
  ([^NormStream handle]
   (.close handle)))

(defn ^Integer write-stream
  "Writes the specified buffer to the stream returning the number of bytes
  placed into the outgoing buffer"
  ([^NormStream handle ^bytes buffer ^Integer len]
   (.write handle buffer 0 len))
  ([^NormStream handle ^bytes buffer ^Integer offset ^Integer len]
   (.write handle buffer offset len)))

(def flush-mode-map
      {:none NormFlushMode/NORM_FLUSH_NONE,
       :active NormFlushMode/NORM_FLUSH_ACTIVE,
       :passive NormFlushMode/NORM_FLUSH_PASSIVE})

(defn ^NormStream flush-stream
  "flushes the outgoing buffers. When eom is true an end-marker is inserted
  into the outgoing message stream. Flush mode can be :none, :passive and :active"
  [^NormStream handle ^Boolean eom flush-mode]
  (.flush handle eom (get flush-mode-map flush-mode))
  handle)

(defn ^NormStream set-auto-flush
  [^NormStream handle flush-mode]
  (.setAutoFlush handle (get flush-mode-map flush-mode))
  handle)

(defn ^NormStream set-push-enable
  "Configures the way this stream manages full buffers. When push-enable is false
  (default) no data will be written to the buffer if it is still occupied by unsent
  data or data in repair. With push-enable set the oldest data will be discarded and
  overwritten."
  [^NormStream handle ^Boolean push-enable]
  (.setPushEnable handle push-enable)
  handle)

(defn ^Boolean has-vacancy?
  "This function is useful in combination with push-enable activated as it
  will check the stream's buffer for space to accept a subsequent write"
  [^NormStream handle]
  (.hasVacancy handle))

(defn ^NormStream mark-eom
  "Sets an EOM marker at the current position of the outbound stream
  allowing consumers and the protocol engine with separations or logical
  data units"
  [^NormStream handle]
  (.markEom handle)
  handle)

(defn ^Boolean set-watermark
  "Sets a watermark and initiates the acknowledgement process with all receivers.
  The override-flush paramter forces the engine to flush all outstanding transmission
  before the acknowledgement process is started. The acknowledgement process is executed
  asynchronously. A :tx-watermark-completed event is fired when all receivers have
  acknowledged. The return value indicates if the watermark could be set."
  [^NormSession handle ^NormObject object ^Boolean override-flush]
  (.setWatermark handle object override-flush))

(defn ^Boolean cancel-watermark
  "Cancels the watermarking process"
  [^NormSession handle]
  (.cancelWatermark handle))

(defn ^Boolean add-acking-node
  "Adds the specified node to the list of nodes participating in the positive
  acknowledgement process"
  [^NormSession handle ^Long node-id]
  (.addAckingNode handle node-id))

(defn ^Boolean remove-acking-node
  "Removes the specified node from the list of nodes participating in the
  positive acknowledgement process"
  [^NormSession handle ^Long node-id]
  (.removeAckingNode handle node-id))

(defn get-acking-status
  "Returns the status of the specified not in the acknowledgement process as
  :invalid the node id is not on the acking list
  :failure the node has not responded or not all addressed nodes have answered
  :pending the node has not responded yet
  :success the node has acknowledged
  handle is the NORM session handle.
  node-id is the NORM node handle of the remote node."
  ([^NormSession handle ^Long node-id]
   (case (.getAckingStatus handle node-id)
     NormAckingStatus/NORM_ACK_FAILURE :failure
     NormAckingStatus/NORM_ACK_INVALID :invalid
     NormAckingStatus/NORM_ACK_PENDING :pending
     NormAckingStatus/NORM_ACK_SUCCESS :success))
  ([^NormSession handle]
   (get-acking-status handle NormNode/NORM_NODE_ANY)))

(defn ^Boolean send-command
  "Sends an application defined command. A :tx-cmd-sent event is generated
  when the command has been sent (incl. repetitions for robustness). Returns
  true if the command has been sent and false otherwise. Sending commands could
  fail if the command size exceeds the session's segment size, the session
  does not have a started sender or a preceding command has not been sent yet.
  handle is the NORM session handle
  buffer is a byte array containing the command data.
  length is the number of bytes used in the buffer.
  robust shall be true to indicate that NORM will make sure that all receivers
  have received the command and false otherwise."
  [^NormSession handle buffer length robust]
  (assert (<= length 256) "Command max size 256 bytes")
  (.sendCommand handle buffer 0 length robust)
  handle)

(defn ^NormSession cancel-command
  [^NormSession handle]
  (.cancelCommand handle))

;;
;; Receiver functions
;;

(defn start-receiver
  "Starts a receiver in the given session. The session starts to participate in
  message exchange and will generate receiver specific events."
  [^NormSession handle ^Long buffer-space]
  (.startReceiver handle buffer-space))

(defn ^NormSession stop-receiver
  "Stops the receiver. The receiver is stopped immediately and all recources
  are freed."
  [^NormSession handle]
  (.stopReceiver handle)
  handle)

(defn ^NormSession set-rx-cache-limit
  "Sets a limit on the number of unconfirmed objects per sender."
  [^NormSession handle ^Integer count-max]
  (.setRxCacheLimit handle count-max)
  handle)

(defn ^Boolean set-rx-socket-buffer
  "Sets an application specific socket buffer for this session"
  [^NormSession handle ^Integer buffer-size]
  (.setRxSocketBuffer handle buffer-size))

(defn ^NormSession set-silent-receiver
  "Configures the receiver to participate in communication without sending
  any feedback messages. The sender will rely entirely on pro-active error
  correction data via FEC for message completeness. The max delay argument
  controls the amount of FEC data kept by the receiver for repair of data."
  ([^NormSession handle ^Boolean silent ^Integer max-delay]
   (.setSilentReceiver handle silent max-delay)
    handle)
  ([^NormSession handle ^Boolean silent]
   (set-silent-receiver handle silent -1)
    handle))

(defn ^NormSession set-default-unicast-nack
  "With unicast nacking enabled the receiver uses unicast to provide
  feedback to the sender. Otherwise the sender uses multicast."
  [^NormSession handle ^Boolean enable]
  (.setDefaultUnicastNack handle enable)
  handle)

(defn ^NormNode set-unicast-nack
  "Sets unicast nacking for the given sender node."
  [^NormNode handle ^Boolean enable]
  (.setUnicastNack handle enable)
  handle)

(defn ^NormSession set-default-sync-policy
  "Sets the syncing behaviour of the client. :current receives all objects
  currently in transmission and new objects sent later. :all requests all
  objects held in sender buffers for retransmission and :stream ensures
  reception of all data stored in the current stream"
  [^NormSession handle sync-policy]
  (.setDefaultSyncPolicy handle (case sync-policy
                                  :current NormSyncPolicy/NORM_SYNC_ALL
                                  :stream  NormSyncPolicy/NORM_SYNC_STREAM
                                  :all     NormSyncPolicy/NORM_SYNC_ALL))
  handle)

(defn- ^NormNackingMode key->nacking-mode
  "Maps the keywords :none, :info-only and :normal used in the clojure world
  to the Java constants used internally"
  [nacking-mode]
  (case nacking-mode
    :none NormNackingMode/NORM_NACK_NONE
    :info-only NormNackingMode/NORM_NACK_INFO_ONLY
    :normal NormNackingMode/NORM_NACK_NORMAL))

(defn ^NormSession set-default-nacking-mode
  "Configure the operation of negative acknowledgment for all new senders.
  :none no feedback is given to senders.
  :info-only repair requests are only sent for INFO-messages
  :normal (default) repair requests are sent for the entire object"
  [^NormSession handle nacking-mode]
  (.setDefaultNackingMode handle (key->nacking-mode nacking-mode))
  handle)

(defmulti set-nacking-mode
          "Configure the nacking mode for the specified sender or object as
          :none No repair requests are sent to this sender.
          :info-only repair requests are sent only for INFO messages.
          :normal repair requests are sent for the entire object."
          (fn [target _] (class target)))

(defmethod set-nacking-mode NormNode
  [^NormNode handle nacking-mode]
  (.setNackingMode handle (key->nacking-mode nacking-mode)))

(defmethod set-nacking-mode NormObject
  [^NormObject handle nacking-mode]
  (.setNackingMode handle (key->nacking-mode nacking-mode)))

(defn- ^NormRepairBoundary key->repair-boundary
  [repair-boundary]
  (case repair-boundary
    :block NormRepairBoundary/NORM_BOUNDARY_BLOCK
    :object NormRepairBoundary/NORM_BOUNDARY_OBJECT))

(defn ^NormSession set-default-repair-boundary
  "Configures the repair process to handle error correction at FEC block boundaries
  (:block) or at object boundaries (:object)"
  [^NormSession handle repair-boundary]
  (.setDefaultRepairBoundary handle (key->repair-boundary repair-boundary))
  handle)

(defn ^NormNode set-repair-boundary
  "Configures the repair process for a specific sender node to handle error correction
  at FEC block boundaries (:block) or at :object level."
  [^NormNode handle repair-boundary]
  (.setRepairBoundary handle repair-boundary)
  handle)

(defn ^NormSession set-default-rx-robust-factor
  "The rx robust factor controls how long the receiver will request repair
  of incomplete or missing transmissions. rx robust factor is multiplied
  with 2xgrtt to give the timeout. A rx robust factor of -1 will have the
  receiver wait indefinitely for repairs"
  [^NormSession handle ^Integer rx-robust-factor]
  (.setDefaultRxRobustFactor handle rx-robust-factor)
  handle)

(defn ^NormNode set-rx-robust-factor
  "The rx robust factor controls how long the receiver will request repair
  of incomplete or missing messages from this sender node."
  [^NormNode handle ^Integer rx-robust-factor]
  (.setRxRobustFactor handle rx-robust-factor)
  handle)

(defn ^Integer read-stream
  "Reads data from a receiver stream. This function is usually called in response
  to a :rx-object-new and :rx-object-updated event. buffer and buffer-size specify
  the location and the size of the buffer allocated to receive the data. The function
  returns the amout of data actually read. This is a non-blocking call. When no data is
  available it will return 0."
  [^NormStream handle ^bytes buffer ^Integer buffer-size]
  (assert (some? buffer) "Buffer must not be null")
  (assert (bytes? buffer))
  (.read handle buffer 0 buffer-size))

(defn ^Boolean seek-message-start
  "Advances the read position to the next message separator item (EOM) on the
  stream. Returns true if an eom was found and false otherwise"
  [^NormStream handle]
  (.seekMsgStart handle))

;;
;; Norm Objects and Data
;;

(defn get-type
  "Returns the type of the object received."
  [^NormObject handle]
  (case (.getType handle)
    NormObjectType/NORM_OBJECT_DATA :data
    NormObjectType/NORM_OBJECT_FILE :file
    NormObjectType/NORM_OBJECT_NONE :none
    NormObjectType/NORM_OBJECT_STREAM :stream))

(defn get-info
  "Get the info data included in the object. Returns a byte array."
  [^NormObject handle]
  (.getInfo handle))

(defn ^Long get-size
  "Get the size of the data contents in bytes"
  [^NormObject handle]
  (.getSize handle))

(defn ^Long get-bytes-pending
  "Returns the number of bytes not yet received from the sender for the given object"
  [^NormObject handle]
  (.getBytesPending handle))

(defn ^NormObject cancel
  "Cancel transmission of the given object. Any pending data transmission is aborted."
  [^NormObject handle]
  (.cancel handle)
  handle)

(defn ^NormObject retain
  "Keep the object in memory and do not purge it, even if buffer thresholds or timeouts
  are exceeded. As this blocks valuable resoures the caller is obliged to release the
  object as soon as possible"
  [^NormObject handle]
  (.retain handle)
  handle)

(defn ^NormObject release
  "Release the buffer space occupied by the object which has been frozen by a
  previous retain call"
  [^NormObject handle]
  (.release handle)
  handle)

(defn ^NormNode get-sender
  "Returns the identity of the sender node of the object."
  [^NormObject handle]
  (.getSender handle))

(defn get-data
  "Returns the data payload of the object as a byte array."
  [^NormData handle]
  (.getData handle))

;;
;; NORM Nodes
;;

(def ^:const NORM_NODE_NONE
  "Wildcard used when ACKing is enforced. Should have been a constant in NormNode just like NORM_NODE_ANY."
  0)

(defn ^Long get-node-id
  "Return the node id of a given node."
  [^NormNode handle]
  (.getId handle))

(defn ^InetSocketAddress get-address
  "Returns the socket address of a given node."
  [^NormNode handle]
  (.getAddress handle))

(defn ^Double get-grtt
  "Returns the specific grtt in seconds for a given node."
  [^NormNode handle]
  (.getGrtt handle))

(defn get-command
  "Returns the command sent by a sender. This function should be called in response
  to a :rx-cmd-new event. Returns nil if the command was larger than 256 bytes or if
  no command was present"
  [^NormNode handle]
  (let [cmd (byte-array 256)
        len (.getCommand handle cmd 0 256)]
    (if (< len 256)
      (let [result (byte-array len)]
        (System/arraycopy cmd 0 result 0 len)
        result)
      cmd)))

(defn ^NormNode free-buffers
  "Returns the buffers associated with a given node. New buffers will be allocated
  when the node starts to communicate again."
  [^NormNode handle]
  (.freeBuffers handle))

(defn get-node-name
  "Returns a readable representation of the node consisting
  of the socket address and the node id"
  [^NormNode node]
  (if node
    (str (get-address node)
         "/"
         (get-node-id node))
    ""))

;;
;; Debug
;;

(defn set-debug-level
  "Set the debug level which is a value between 0 (= off) and 12 (highest).
  Debug output is written to stdout unless redirected by a call to open-debug-log"
  [^NormInstance handle level]
  (.setDebugLevel handle level))

(defn open-debug-log
  "Opens a file for debug output. Throws an IOException if the file cannot be opened"
  [^NormInstance handle ^String filename]
  (.openDebugLog handle filename))

(defn close-debug-log
  "Closes the debug file. All further debug output will go to stdout."
  [^NormInstance handle]
  (.closeDebugLog handle))

