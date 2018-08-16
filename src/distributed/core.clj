(ns distributed.core
  (:require [distributed.netio :as nio :refer [remote leader-port leader-host data ->>>]]))
(defn rf [] (require 'distributed.core :reload)) 

(def a 1)
(def b 6)
(defn qqq [] (println "Test"))

;;Need to require current ns in netio to be able to call functions for RPC
(in-ns 'distributed.netio)
(require '[distributed.core :refer :all])
(ns distributed.core)
