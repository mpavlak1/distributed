(ns distributed.netio
  (:require [clojure.set]))
(defn rf [] (require 'distributed.netio :reload))

;;A namespace for evaluating functions remotely on clojure instances across the network

(def debug true) ;;when debug true, use local host only (for testing and devlopment)
(def timeout 3000) ;;time to wait for remote calls
(def heart-beat-time 3000) ;;time to wait on each heart beat

;;Returns InetAddress of local host (127.0.0.1)
(defn local-host [] (java.net.InetAddress/getByName nil))

;;Returns InetAddress of local address
(defn host-address [] (java.net.InetAddress/getLocalHost))

(defn ip->host [ip] (java.net.InetAddress/getByName (if debug nil ip)))

(defn server-socket [port] 
  (let [ss (java.net.ServerSocket. port 0 (if debug (local-host) (host-address)))] ;;use host-address
    (.setSoTimeout ss timeout)
    ss))

(defn ->socket [port host] 
  (let [s (java.net.Socket. host port)]
    (.setSoTimeout s timeout)
    s))

(defn write-to-socket [s val]
  (with-open [out (java.io.ObjectOutputStream. (.getOutputStream s))]
    (.writeObject out val)
    (.flush out))
  (.close s))

(defn read-from-socket [s]
  (with-open [in (java.io.ObjectInputStream. (.getInputStream s))]
    (let [r (.readObject in)] 
      (.close s)
      r)))

(defn rand-port-number [] (+ 1025 (rand-int 9000)))
(defn ->server-socket []
  (let [r (rand-port-number)]
    (println "Listening on port: " r)
    (try (server-socket r)
      (catch java.net.BindException e nil))))

(def start true)

(def messages (atom [])) ;;Container to put recived messages
(def listen? (atom start)) ;;Continous threads will run while true
(def ssock (atom (when start (->server-socket))))

(defn listen-for-messages [self-socket]
  ;;Create new background thread to constantly listen for connecting sockets
  ;;Accepts connections and pushes messages onto stack
  (future (while @listen?
            (try
              (swap! messages conj (read-from-socket (.accept self-socket)))
              (catch java.net.SocketTimeoutException e (Thread/sleep 100)))))) ;;wait after timeout

;;Removes and returns the newest message
(defn pop-message []
  (dosync 
    (when (not (empty? @messages))
      (let [r (last @messages)]
        (swap! messages pop)
        r))))

(def remote-returns (atom {})) ;;key = messageId, val = evaluation from remote
(def waiting (atom {})) ;;Waiting: key = messageID, val = java.util.concurrent.CountDownLatch
(def id (atom 0))

;;Continually evaluates all messages on message stack
;;Assumes message results have side effects, no values returned
(defn eval-messages []
  (future (while @listen? ;;Might be able to use watcher here instead, but this works for now
            (let [m (pop-message)]
              (if (:return m)
                (write-to-socket (->socket (:port m) (:host m))
                  {:id (:id m) 
                   :fn (list 'swap! 'remote-returns 'assoc (:id m) (try (eval (:fn m))
                                                                     (catch Exception e (.getMessage e))))})
                (try (eval (:fn m)) (catch Exception e (.getMessage e)))) ;;Exceptions in background threads not displayed to REPL
              (Thread/sleep 100)))))

;;Removes old values from remote-returns
;;Decreases latch-lock to 0 to notify that result is avialable
(defn remove-remote-returns []
  (add-watch remote-returns :remote-remover
    (fn [k r old new]
      (let [i @id]
        (doseq [k (filter #(< % (dec i)) (keys @remote-returns))]
          (swap! remote-returns dissoc k)) ;;don't remove last
        (.countDown (get @waiting (dec i)))))))

;;Background threads and watchers for async
(def message-listener (when @listen? (listen-for-messages @ssock))) ;;(future-cancel message-listener) to kill thread
(def message-evaluator (when @listen? (eval-messages)))
(def remote-remove (when @listen? (remove-remote-returns)))

;;Waits for latch lock to be released on result id or for timeout (returns nil on timeout)
(defn wait-for-return [id max-wait-ms]
  (let [t (future (Thread/sleep max-wait-ms) (.countDown (get @waiting id)))]
    (.await (get @waiting id))
    (let [r (get @remote-returns id)]
      (swap! waiting dissoc id)
      r)))

;;literal-fn is a list of symbols that can be evaluated as a function
;;requires quoting fn to delay evaluation on local prior to sending to remote
;;Example: if a is defined remotely as 7; (list '+ 'a 10) will return 17.
(defn remote-eval [port host literal-fn & {:keys [return] :or {return true}}]
  (let [i @id] ;;deref id at start, other threads can update
    (swap! waiting assoc i (java.util.concurrent.CountDownLatch. 1)) ;;Add latch lock for id result
    (write-to-socket (->socket port host) ;;Send message to remote
      {:return return
       :port (.getLocalPort @ssock)
       :host (if debug (local-host) (java.net.InetAddress/getByName
                                      (str (.getInetAddress @ssock))))
       :id i
       :fn literal-fn})
    (dosync (swap! id inc)) ;;increase and return id
    i))

;;Evalulates fn on remote host then returns the result
(defn remote [port host literal-fn & {:keys [return] :or {return true}}]
  (wait-for-return (remote-eval port host literal-fn :return return) (if return timeout 0)))


;;=============== TRANSACTIONS ===============

;;All transactions commit data to 'data' which is a hash-map atom

;;Port and Host of leader
(def leader (atom nil))

;;Set of connections [Port Host]
(def connections (atom #{}))

(defn add-connection [port host]
  (reset! connections (into @connections (hash-set [port host])))
  [(.getLocalPort @ssock) (str (.getInetAddress @ssock))])

(defn remove-connection [port host]
  (reset! connections (clojure.set/difference @connections (hash-set [port host])))
  true)

;;Connect to leader on host/port
(defn connect [port host]
  (let [r (remote port host 
            (list 'add-connection (.getLocalPort @ssock) 
              (list 'ip->host (str (.getInetAddress @ssock)))))]
    (when r (reset! leader r)))) 

;;Disconnect for registered leader, does not leave network, just resets leader
(defn disconnect []
  (reset! leader nil))

;;=============== Election Coordination =============== 

;;Election process function should be call back for missed heart beats to lookup or elect leader
;;Dumb implementation of a persistent network using LEADER file as shared state.
;;Real implementation should use zookeeper/paxos for leader consensous

;;Dumb implemenation has potential infinite election cycles

(def leader-file (str "." (java.io.File/separator) "LEADER"))

;;Checks leader file for leader or creats it if dose not exist
(defn ->leader-file []
  (if (.exists (clojure.java.io/as-file leader-file))
      (let [e (clojure.string/split (slurp leader-file) #"\t")]
        [(read-string (first e)) (last e)])
      (do
        (spit leader-file (str (.getLocalPort @ssock) "\t" (str (.getInetAddress @ssock))))
        [(.getLocalPort @ssock) (str (.getInetAddress @ssock))])))

;;Function that node calls when lose role as leader node
;;Tells all clients to disconnect and preform a new election
(defn leave-leadership [new-leader]
  (->leader-file)
  (let [old-connections @connections]
    (doall (for [c old-connections] (remote (first c) (second c) 
                                      (list 'reset! 'leader [(first new-leader) (list 'ip->host (second new-leader))] :return false))))
    (remote (first new-leader) (ip->host (second new-leader)
                                 (list 'reset! 'connections
                                   (list 'map '(fn [x] (vector (first x) (ip->host (second x))))
                                     (vec (map (fn [y] (vector (first y) (str (second y)))) old-connections))))))) 
  (reset! leader (first new-leader) (ip->host (second new-leader))))

;;Checks file to see if self is leader
(defn dumb-leader-check [leader-file]
  (when (.exists (clojure.java.io/as-file leader-file)) 
    (let [e (clojure.string/split (slurp leader-file) #"\t")]
         (and (= (last e) (str (.getInetAddress @ssock)))
              (= (read-string (first e)) (.getLocalPort @ssock))))))

;;Dumb implementation of election process, replace with real election coordinating function
;;Attempts to read leader file. If it dose not exist, creates it and becomes leader node
;;Leader file contains [Port ip-address of leader]
(defn dumb-election [leader-file]
  (let [new-leader (->leader-file)]
    (if (dumb-leader-check leader-file)
      (do (future (do (while (dumb-leader-check leader-file) (Thread/sleep heart-beat-time)) 
                      (leave-leadership new-leader)))
        new-leader)
      (do          
        (when (not (try (remote (first new-leader) (ip->host (second new-leader)) 'true) 
                        (catch Exception e nil)))
          (do (.delete (clojure.java.io/as-file leader-file))
              (dumb-election leader-file)))
        (println "CONNECTING TO LEADER" new-leader)
        (try 
          (connect (first new-leader) (ip->host (second new-leader)))
          (catch Exception e (do (println "Re-trying election") 
                               (.delete (clojure.java.io/as-file leader-file))
                               (dumb-election leader-file))))))))

(defn leader-port [] (if @leader 
                       (first @leader) 
                       (do (reset! leader (dumb-election leader-file))
                           (leader-port))))

(defn leader-host [] (if @leader 
                       (ip->host (second @leader))
                       (do (reset! leader (dumb-election leader-file))
                           (leader-host))))

;;Returns true when leader is equal to self
(defn self-leader? []
  (and (= (.getLocalPort @ssock) (leader-port))
       (= (.getInetAddress @ssock) (leader-host))))

;; =============== Consistent Commits ===============

(def data (atom {}))

;;Solicits votes on if all clients can commit
(defn can-commit? [key val]
  (every? identity
    (doall
      (map deref 
        (map #(future (remote (first %) (second %)
                        '(not= nil (assoc @data key val))))
          @connections)))))

;;Attempt to make commits, 
;;If client times out responding to leader, abort
;;If leader times out responding to client, commit (do nothing)
(defn pre-commit [key val]
  (every? identity
    (doall 
      (map deref
        (map #(future (remote (first %) (second %)
                        (list 'when (list 'swap! 'data 'assoc key val) true))) 
          @connections)))))

;;Tell clients to undo commit
(defn abort [key val]
  (if (self-leader?)
    (doseq [c @connections]
      (future (remote (first c) (second c) (list 'when (list 'swap! 'data 'dissoc key)))))
    (remote (leader-port) (leader-host) (list 'abort key val)))
  (swap! data dissoc key))

;;Veryify that all clients have commited
;;If client response times out, issue abort
;;If leader to client times out, assume commit
(defn finalize-commit [key val]
  (every? identity 
    (doall 
      (map deref 
        (map #(future (remote (first %) (second %) (list 'contains? '@data key)))
          @connections)))))

;;Issue a 3-phase transaction to commit key-val on all clients
;;If not all clients can commit, aborts attempt
;;Clients acknoledge commit request to leader after each stage
;;Leader timeout to client on pre-commit and final commit causes clients to commit
;;Leader timeout to client on vote soliciting causes abort
;;Any client timeout to leader causes abort
(defn three-phase-commit [key val]
  (let [r (if (can-commit? key val)
            (if (pre-commit key val)
              (if (finalize-commit key val)
                true
                (abort key val))
              (abort key val)))]
    ;;Add transaction on leader when successfully commited to clients
    (when r (do (swap! data assoc key val) true))))

;;Need alternative way for clients to issue commits
;;Can't directly call remote three-phase-commit because remote is thread blocking.
;;   This results in can-commit?/pre-commit/finalize-commit to always return nil for client calling remote
;;   Which results in timeout which cause leader to abort the transaction
(def pending-commits (atom []))
(defn process-commits []
  (add-watch pending-commits :p
    (fn [k r old new]
      (when (not (empty? new))
        (let [c (last new)]
          (cond
            (= :abort (first c)) (future (abort (second c) (last c)))
            (= :3pc (first c)) (future (three-phase-commit (second c) (last c)))))
        (swap! pending-commits pop)))))
(process-commits)

;;Clients can ask leader to issue three phase commit on behalf of the client
(defn request-3pc [key val]
  (remote (leader-port) (leader-host)
            (list 'swap! 'pending-commits 'conj [:3pc key val]) :return false))

;; =============== Heart Beat Protocol ===============

;;Pings the node every beat-time seconds
;;Once timeout occurs, calls the timeout-fn
(defn heart-beat [port host beat-time & {:keys [timeout-fn] :or {timeout-fn #(identity nil)}}]
  (future
    (do
      @(future (while (and @listen? (every? identity (with-redefs [timeout beat-time]
                                                       [@(future (do (Thread/sleep beat-time) true))
                                                        (remote port host 'true)])))))
      (timeout-fn))))

;;When leader node is updated (non nil), start a heart-beat protocol with the new leader
;;On disconnect, try to inform leader of disconnect and reset leader to nil
(defn start-heart-beat-on-new-leader [beat-time]
  (add-watch leader :leader-watch
    (fn [k r old new]
      (when (and new (not (self-leader?)))
        (heart-beat (first new) (ip->host (second new)) beat-time 
          :timeout-fn #(do 
                         (Thread/sleep (* 2 heart-beat-time))
                         (connect (leader-port) (leader-host))))))))
(start-heart-beat-on-new-leader heart-beat-time)

;;Starts up a heart-beat protocol when connecting to another node
;;When heart beat fails, tried to tell connection to disconnect and
;; removes missed heart beat connection from connection list
(defn start-heart-beat-on-connect [beat-time]
  (add-watch connections :new-connections 
    (fn [k r old new]
      (let [c (first (clojure.set/difference new old))]
        (when c
          (heart-beat (first c) (second c) beat-time 
            :timeout-fn #(do nil))))))) 

(defn rejoin-network []
  (reset! leader nil)
  (connect (leader-port) (leader-host)))

;;Leave network and don't attempt to re-connect, remove self from connections list
(defn leave-network []
  (remote (leader-port) (leader-host) ;;Remove self from connection list of leader
    (list 'remote-connections (.getLocalPort @ssock)
      (list 'ip->host (str (.getInetAddress @ssock)))))
  (disconnect))

(defn start-up []
  (reset! leader (dumb-election leader-file))
  (connect (leader-port) (leader-host)))
(when start (start-up))

;;Kill all background threads on shutdown
(defn shutdown []
  (reset! listen? false)
  (future-cancel message-listener)
  (future-cancel message-evaluator)
  (remove-watch remote-returns :remote-remover)
  (remove-watch pending-commits :p)
  (remove-watch leader :leader-watch)
  (remove-watch connections :new-connections))

;; =============== Distributed Mapping ===============

(defn ->connections [] 
  (if (self-leader?)
    (vec (map (fn [x] (vector (first x) (str (second x)))) @connections))
    (remote (leader-port) (leader-host) '(->connections))))

;;Function to pass literal-fn to leader to eval
(defn ->>> [literal-fn] 
  (eval (list 'remote (list 'leader-port) (list 'leader-host) (list 'quote literal-fn))))

(defn partition-work [seq connections]
  (let [n (count seq) c (count connections)
        parts (partition (int (/ n c)) seq)
        left-over (concat (take-last (mod n c) seq) (repeat (- c (mod n c)) nil))]
    (map #(filter identity (flatten %)) (vec (zipmap parts left-over)))))

(defn ->workers-literals [fn seq connections & {:keys [pmap] :or {pmap false}}]
  (let [workers (zipmap connections (partition-work seq connections))]
    (for [w (keys workers)]
      [w (list 'list (list 'quote (if pmap 'pmap 'map)) (list 'quote fn) (vec (get workers w)))])))

;;Remote map: Distribute work to connections and return results.
;;Uses same semantics as pmap: results will not be returned in any order - evaluations done in parallel
;;Additioanlly if when processing localally, pmap should be used instead of map, set :pmap as true
;;***When defining anonomous functions, have to use the lamda notation, not the #% macro
;;Example: Does not work   -> (rmap #(+ 1 %) (range 10))
;;         Works correctly -> (rmap '(fn [x] (+ 1 x)) (range 10) 
;;         Also works      -> (rmap 'inc (range 10)
(defn rmap [literal-fn seq & {:keys [pmap max-wait] :or {pmap false max-wait timeout}}]
  (let [evals (->workers-literals literal-fn seq 
                (map #(vector (first %) (ip->host (second %))) (->connections)) :pmap pmap)]
    (reduce concat 
      (map deref (map #(future (remote (ffirst %) (second (first %)) (last %))) evals)))))


