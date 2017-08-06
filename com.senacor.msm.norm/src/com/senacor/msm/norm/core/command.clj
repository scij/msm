(ns com.senacor.msm.norm.core.command
  (:require [bytebuffer.buff :as bb]
            [clojure.string :as str]))


(def ^:const CMD_SF_ALIVE
  "A stateful process sends this command to inform it's peers that it is still alive and processing" 1)
(def ^:const CMD_SF_ACTIVE
  "A stateful process sends this command to claim the active role in the stateful group. Passive
  peers send this message to confirm the active role of another peer" 2)
(def ^:const CMD_SL_PROCESSED
  "A stateless process sends this command to inform it's peers that it has processed a given message" 3)

;; Command Structure
;;
;; Numeric values are in network byte order
;;
;; magic-number 2 bytes "CX"
;; major version 1 byte
;; minor version 1 byte
;; payload-length short
;; -- end of fixed part (6 bytes)
;; payload payload-length bytes
;; -- end of message

(defn length
  "Returns the length of the command array in bytes for a given command payload string"
  [payload]
  (+ 6 (count payload)))

(defn alive [node-id subscription])

(defn active [node-id subscription])

(defn processed [node-id uuid])