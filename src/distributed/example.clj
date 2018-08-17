(ns distributed.example
  (:require [distributed.netio :as nio :refer [remote leader-port leader-host data ->>> rmap]]))
(defn rf [] (require 'distributed.core :reload))

;;This is a namespace that will show examples of how to use functions in netio

;;Finding the leader node:
(println "Leader Node: " (leader-port) (str (leader-host)))
;;leader-port returns an int for the port that the leader is using
;;leader-host returns the inetaddress of the leader

;;Node details
(println "Current Node: " (.getLocalPort @nio/ssock) (str (.getInetAddress @nio/ssock)))
;;each node as a server socket object that it uses to accept connections from other nodes

;;Telling leader to print message for sender
(println (remote (leader-port) (leader-host) '(println "Hello Remote") :return false))

;;If this message prints to the sender, then the sender is the leader. 
;;Can check if current node is leader using self-leader? pred
(println (str "Current node is leader: " (nio/self-leader?)))

;;Making remote calls (RPC)
;;The remote function takes arguments port, host (inetaddress obj), and functions to call
;; the optional :return argument will cause the caller to wait for the reponse or timeout when true
;; when :return is false, the function will be evaluated remotely and not returned to sender (used for side-effects)

;;Evaluating expressions remotely
;;Adding 1 + 2 on leader node
(println (str "Evaluated Remotly: " (remote (leader-port) (leader-host) '(+ 1 2))))

;;Note that the function argument has to be delayed using quotes

;;The entire function argument do not necesarrly have to be quoted. 
;;More complex functions can be built with pre-evaluated values send to remote to use as arguments

;;Using arguments on local machine in function called on remote:
(def a 123) ;;Some local argument
(println (str "Evaluated Remotly with local arguments: " 
           (remote (leader-port) (leader-host) (list '+ a 456))))

;;The ->>> function is short-hand for calling a function remotely on leader node
(println (str "Same thing: " (->>> (list '+ a 456))))

;;To use RPC with variables and functions from another ns, they have to be defined in netio
(in-ns 'distributed.netio)
(require '[distributed.example :refer [a]])
(ns distributed.example)

;;A combination of local and remote variables can be used
(println (str "Local and Remote args: " (->>> (list '+ a 'a))))
;;Returns 2*a if only one node online and current node is leader, however,
;;First the message to evaluate the function (+ 'a 123) is send to leader (which is the current node)
;;That message is recived and evaluated (+ 'a 123) -> (+ 123 123) -> 246
;;246 is then send back to the caller (in this case from self to self)

;;Setting the :return option to false will tell the sender to not wait for a response.
;;This is used for calling functions that have side-effects and always return nil
(remote (leader-port) (leader-host) '(println "Caller won't wait for this response: " (+ a a)) :return false)

;;If functions take to long to evaluate, the will timeout and return nil
;;The timeout variable is used to set the maximum wait time for a function to wait before assuming it won't return
(println "Maximum wait time for return on remote: " nio/timeout)
(println "This will always timeout: "
  (remote (leader-port) (leader-host) '(do (Thread/sleep (+ timeout timeout)) true)))

;;Remote calls are evaluated in the background on the remote machine.
;;The :return false argument can be used to do long functions with side effects without waiting to the nil return
(println "Telling leader to do some long process: " 
  (remote (leader-port) (leader-host) 
    '(do (Thread/sleep (+ timeout timeout)) (println "Long process done.")) :return false))

;;To better show the rest of the functionality of netio, start a new REPL process
;;From this point on X will refer to the leader node and Y a follower node in some other REPL process
;;These example will be run with the debug variable set to true, meaning the network is only on the local-host 
;; where each new node runs on a random port

;;To re-show the difference between remote and local variables through delayed evaluation, 
;;redfine a on X to be 111 and a on Y to be 222
(def a (if (nio/self-leader?) 111 222))

;;In either X or Y
(doseq [c (nio/->connections)] 
  ;;->connections returns @connections if leader, otherwise ask leader for connections list
  (println "Remote vs Local variables: " 
    (remote (first c) (nio/ip->host (second c)) (list '+ 'a a))))
;;Leader's a + Local a
;;Notice how when X calls the function, 222 is returned because Leader and Local a are both 111
;;       but when Y calls the function, 333 is returned beacuse Leader a = 111 and Local a = 222

;;Each instance of netio has a hash-set @data
;;This hash-set is consitent across all nodes on the same network using 3-phase-commit transactions
(println @nio/data)

;;The request-3pc function is used to make a consistent commit to this map
(nio/request-3pc :abc 123)

;;Verify that all nodes got the update
(println "Verify @data for all nodes: "
  (for [c (nio/->connections)]
    (remote (first c) (nio/ip->host (second c)) '@data)))

;;To remove a value from all @data nodes
(nio/abort :abc nil)

;;Might need to wait a maximum of *timeout* for tranaction to be finalized:
;;Verify all nodes aborted the value
(println "Verify :abc was removed for all nodes: "
  (for [c (nio/->connections)]
    (remote (first c) (nio/ip->host (second c)) '@data)))

;;Mapping and distributing work to nodes across the network can be done using the rmap function
;;The rmap function is similar to the pmap function except mapping is done on remote machines rather than different threads
;;There is no gauranteed order for remote returns - results always be assumed as unsorted.

;;Note: the function being mapped has to be quoted (delayed evaluation)
(println "Doing work on multiple machines"
  (rmap 'inc (range 7)))

;;Functions can be expressed a number of ways just like normal map:
(println "Different syntax: "
  (rmap 'inc (range 7))
  (rmap '(fn [x] (+ 1 x)) (range 7))
  (rmap '#(+ 1 %) (range 7)))

;;The :return false option can be used to tell the remote functions used internally to not wait for a response
(rmap '#(println %) (range 7) :return false)
;;You should see about half the numbers printed on X and the other half on Y
;;Just like the normal map function, mapping a function with side effect will return a sequence of all nil.

;;Additionally, the :pmap [true/false] option can be used to tell the remote node to use multiple threads when mapping locally
(time (let [x1 (rmap #(do (Thread/sleep 1000) (println %)) (range 11) :pmap true)]))
(time (let [x2 (rmap #(do (Thread/sleep 1000) (println %)) (range 11) :pmap false)]))

;;When pmap is true, the work is spread accross multiple threads, so the delay is 1 second * number of threads
;;When pmap is false, the work is done on a single thread which waits the 1 second delay for each value it maps to.


;;Heart-beat protocols are used to check that there is still a valid connection between two machines
;;The heart-beat function is used to set up this protocol

;;The function takes the arguments of port, host, beat-time (how long between beats)
;;The :timeout-fn will be called when a heart-beat is missed and the protocol terminates
(doseq [c (nio/->connections)]
  (nio/heart-beat (first c) (nio/ip->host (second c)) 10 
    :timeout-fn (println [(first c) (second c)] ": Missed heart-beat")))
;;The timeout time is set low to gaurantee a heart-beat miss on the first try
;;Realistically, the beat time should be ~2X the timeout time

(def old-connections (nio/->connections))
;;Typically heart-beats are used to disconnect from the server when a node identifies it can no longer reliably communicate
(doseq [c (nio/->connections)]
  (nio/heart-beat (first c) (nio/ip->host (second c)) 10 
    :timeout-fn (do (nio/leave-network)
                    (println [(first c) (second c)] ": disconnected."))))

;;Nodes can reconnect to the network using the connect function
(doseq [c old-connections]
  (remote (first c) (nio/ip->host (second c)) (nio/connect (leader-port) (leader-host))))

;;The leader is coordinated and maintained using an election process.
;;Currently only simple naive implementation is used - this should be replaced with a better implementation of paxos/zookeeper
;;The connect, disconnect, leave-leadership, and 'dumb-election' functions do leader coordination
;;These function should be replaced/overwritten when using more robust implementation

;;Leave the network permently and shutdown
(doseq [c old-connections]
  (try
    (remote (first c) (nio/ip->host (second c)) 
        (do
          (println (first c) (second c) "Leaving permently")
          (nio/leave-network)
          (nio/shutdown)) :return false)
    (catch Exception e nil)))

