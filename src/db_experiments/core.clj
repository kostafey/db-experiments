(ns db-experiments.core
  (:require [clojure.java.jdbc :as j]
            [clojure.string :as s]
            [clojure.reflect :refer [resolve-class]])
  (:import [clojure.lang MapEntry]))

(def db
  {:subname (str "file://"
                 (.replaceAll (System/getProperty "user.home") "\\\\" "/")
                 "/tmp/data;AUTO_SERVER=TRUE")
   :subprotocol "h2"})

(def create-users
  (str
   "CREATE TABLE IF NOT EXISTS users (           \n"
   "  id int(11) NOT NULL AUTO_INCREMENT,        \n"
   "  login varchar(45),                         \n"
   "  email varchar(45),                         \n"
   "  first_name varchar(255) NOT NULL,          \n"
   "  last_name varchar(45) NOT NULL,            \n"
   "  description clob DEFAULT NULL,             \n"
   "  PRIMARY KEY (id),                          \n"
   "  UNIQUE KEY id_UNIQUE (id)                  \n"
   ")                                            \n"))

(defn class-exists? [c]
  (resolve-class (.getContextClassLoader (Thread/currentThread)) c))

(defn is-clob? [x]
  (or (instance? java.sql.Clob x)
      (and (class-exists? 'oracle.sql.CLOB)
           (instance? (Class/forName "oracle.sql.CLOB") x))))

(defn clob-to-string [clob]
  "Turn an Oracle Clob into a String"
  (with-open [rdr (java.io.BufferedReader. (.getCharacterStream clob))]
    (apply str (line-seq rdr))))

(defn clob-to-string-row [row]
  "Check all data in row if it's a CLOB and convert CLOB to string."
  (mapv (fn [x]
          (let [is-map-entry? (instance? MapEntry x)
                value (if is-map-entry? (.val x) x)]
            (if (is-clob? value)
              (if is-map-entry?
                (MapEntry. (.key x) (clob-to-string value))
                (clob-to-string value))
              x)))
        row))

(comment
  (j/execute! db (list create-users))
  (j/execute! db (list (str "INSERT INTO users "
                            "(id, login, first_name, last_name, description) "
                            "VALUES (1, 'admin', 'John', 'Doe', 'First user')")))

  (with-open [conn (j/get-connection db)]
    (let [stmt (j/prepare-statement conn "SELECT * FROM users")]
      (j/query db stmt
               {:as-arrays? true
                ;; :result-set-fn doall
                :row-fn clob-to-string-row
                })))
  )
