(ns mx.interware.arp.io.server
  (:require    
    [manifold.deferred :as d]
    [manifold.stream :as s]
    [clojure.edn :as edn]
    [aleph.tcp :as tcp]
    [gloss.core :as gloss]
    [gloss.io :as io]))

;; Complete documentation for the `aleph.tcp` namespace can be found [here](http://aleph.io/codox/aleph/aleph.tcp.html).

;; ## the basics

;; This uses [Gloss](https://github.com/ztellman/gloss), which is a library for defining byte
;; formats, which are automatically compiled into encoder and streaming decoders.
;;
;; Here, we define a simple protocol where each frame starts with a 32-bit integer describing
;; the length of the string which follows.  We assume the string is EDN-encoded, and so we
;; define a `pre-encoder` of `pr-str`, which will turn our arbitrary value into a string, and
;; a `post-decoder` of `clojure.edn/read-string`, which will transform our string into a data
;; structure.
(def protocol
  (gloss/compile-frame
    (gloss/finite-frame :uint32
      (gloss/string :utf-8))
    pr-str
    edn/read-string))

;; This function takes a raw TCP **duplex stream** which represents bidirectional communication
;; via a single stream.  Messages from the remote endpoint can be consumed via `take!`, and
;; messages can be sent to the remote endpoint via `put!`.  It returns a duplex stream which
;; will take and emit arbitrary Clojure data, via the protocol we've just defined.
;;
;; First, we define a connection between `out` and the raw stream, which will take all the
;; messages from `out` and encode them before passing them onto the raw stream.
;;
;; Then, we `splice` together a separate sink and source, so that they can be presented as a
;; single duplex stream.  We've already defined our sink, which will encode all outgoing
;; messages.  We must combine that with a decoded view of the incoming stream, which is
;; accomplished via `gloss.io/decode-stream`.
(defn wrap-duplex-stream
  [protocol s]
  (let [out (s/stream)]
    (s/connect
      (s/map #(io/encode protocol %) out)
      s)

    (s/splice
      out
      (io/decode-stream s protocol))))

;; The call to `aleph.tcp/client` returns a deferred, which will yield a duplex stream that
;; can be used to both send and receive bytes. We asynchronously compose over this value using
;; `manifold.deferred/chain`, which will wait for the client to be realized, and then pass
;; the client into `wrap-duplex-stream`.  The call to `chain` will return immediately with a
;; deferred value representing the eventual wrapped stream.
(defn client
  [host port]
  (d/chain (tcp/client {:host host, :port port})
    #(wrap-duplex-stream protocol %)))

;; Takes a two-argument `handler` function, which takes a stream and information about the
;; connection, and sets up message handling for the stream.  The raw stream is wrapped in the
;; Gloss protocol before being passed into `handler`.
(defn start-server
  [handler port]
  (tcp/start-server
    (fn [s info]
      (handler (wrap-duplex-stream protocol s) info))
    {:port port}))

;; ## echo servers

;; This creates a handler which will apply `f` to any incoming message, and immediately
;; send back the result.  Notice that we are connecting `s` to itself, but since it is a duplex
;; stream this is simply an easy way to create an echo server.
(defn fast-echo-handler
  [f]
  (fn [s info]
    (println :fast-echo-handler :info info)
    (s/connect
      (s/map f s)
      s)))

;; While we can do trivial computation on the same thread we receive messages, longer computation
;; or blocking operations should be done elsewhere.  To accomplish this, we need something a
;; little more complicated than `connect`ing a stream to itself.
;;
;; Here, we define an asynchronous loop via `manifold.deferred/loop`.  In this loop, we take a
;; message from the stream, transform it on another thread with `manifold.deferred/future`,
;; send it back, and then repeat.

;; Alternately, we use `manifold.deferred/let-flow` to implement the composition of these
;; asynchronous values.  It is certainly more concise, but at the cost of being less explicit.
(defn slow-echo-handler
  [f]
  (fn [s info]
    (d/loop []
      (->
        (d/let-flow [msg (s/take! s ::none)]
          (when-not (= ::none msg)
            (d/let-flow [msg'   (d/future (f msg))
                         result (s/put! s msg')]
              (when result
                (d/recur)))))
        (d/catch
          (fn [ex]
            (s/put! s (str "ERROR: " ex))
            (s/close! s)))))))

;; ### demonstration

;; We start a server `s` which will return incremented numbers, slowly.
(comment def s
  (start-server
    (slow-echo-handler
      (fn [x]
        (println :llego x)
        "ok"))
    10000))



(comment def s
  (start-server
    (fast-echo-handler (fn [p] (println :p p) "ok"))
    10000))

(comment def c @(client "localhost" 10000))

(comment do
  (time @(s/put! c 1))
  (time @(s/put! c 2))
  (time @(s/put! c 3))
  )

(comment do
  (println @(d/timeout! (s/take! c) 10000 :timeout))
  (println @(d/timeout! (s/take! c) 10000 :timeout))
  (println @(d/timeout! (s/take! c) 10000 :timeout))
  )
(defn do-it [n]
  (pmap 
    (fn [n]
      [n (let [cte @(client "localhost" 10000)
               a @(s/put! cte n)
               b @(s/take! cte)]
           (.close cte)
           [a b])]) 
    (range 1 (inc n))))
                    
(defn do-it2 [n]
  (let [cte @(client "localhost" 10000)]
    (doall (map (fn [n]
                  @(s/put! cte n)
                  @(s/take! cte)) (range 1 n)))))           
    


