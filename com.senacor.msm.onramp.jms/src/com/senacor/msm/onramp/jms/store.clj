(ns com.senacor.msm.onramp.jms.store
  (:require [clojure.core.async :refer (go-loop <! !>)]
            [com.senacor.msm.common.msg :as msg])
  (:import (com.senacor.msm.common.msg Message)))

(defn store
  "Store the message in a local repository using the correlation id as the key.
  chan input channel of Message objects."
  [chan]
  (go-loop [msg (<! chan)]
    ;; message unter der correlation id speichern und zusätzlichen Index für
    ;; ein expiry date (sehr kurz) pflegen.
    )
  )
