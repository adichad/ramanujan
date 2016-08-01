CREATE DATABASE IF NOT EXISTS Ramanujan;
USE Ramanujan;

CREATE TABLE IF NOT EXISTS users(
   id INT NOT NULL AUTO_INCREMENT,
   username VARCHAR(100) NOT NULL,
   password VARCHAR(100) NOT NULL,
   PRIMARY KEY ( id )
);

CREATE TABLE IF NOT EXISTS BookMarks(
   id INT NOT NULL AUTO_INCREMENT,
   primarykey VARCHAR(100) NOT NULL,
   bookmark VARCHAR(100) NOT NULL,
   PRIMARY KEY ( id )
);

CREATE TABLE IF NOT EXISTS kafkaBookMarks(
   id INT NOT NULL AUTO_INCREMENT,
   primarykey VARCHAR(100) NOT NULL,
   bookmark VARCHAR(100) NOT NULL,
   PRIMARY KEY ( id )
);

CREATE TABLE IF NOT EXISTS VarTypeRecordsTable (
   tablename VARCHAR(100) NOT NULL,
   colname VARCHAR(100) NOT NULL,
   coltype VARCHAR(100) NOT NULL,
   usertype VARCHAR(100) NOT NULL,
   PRIMARY KEY (tablename,colname)
);

CREATE TABLE IF NOT EXISTS kafkaVarTypeRecordsTable (
   topicname VARCHAR(100) NOT NULL,
   kafkaColname VARCHAR(100) NOT NULL,
   kafkaUsertype VARCHAR(100) NOT NULL,
   PRIMARY KEY (topicname,kafkaColname)
);

CREATE TABLE IF NOT EXISTS RunningLOGS (
   id INT NOT NULL AUTO_INCREMENT,
   host VARCHAR(100) NOT NULL,
   port VARCHAR(40) NOT NULL,
   dbname VARCHAR(100) NOT NULL,
   dbtable VARCHAR(100) NOT NULL,
   runTimeStamp DATETIME NOT NULL,
   hash VARCHAR(100) NOT NULL,
   exceptions VARCHAR(1024) NOT NULL,
   notes VARCHAR(1024) NOT NULL,
   PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS kafkaRunningLOGS (
   id INT NOT NULL AUTO_INCREMENT,
   cluster VARCHAR(100) NOT NULL,
   topic VARCHAR(100) NOT NULL,
   alias VARCHAR(100) NOT NULL,
   groupName VARCHAR(100) NOT NULL,
   runTimeStamp VARCHAR(100) NOT NULL,
   hash VARCHAR(100) NOT NULL,
   exceptions VARCHAR(100) NOT NULL,
   notes VARCHAR(100) NOT NULL,
   PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS Status (
   dbname VARCHAR(100) NOT NULL,
   tablename VARCHAR(100) NOT NULL,
   primarykey VARCHAR(100) NOT NULL,
   qualifiedName VARCHAR(256) not NULL,
   sourceid VARCHAR(100) NOT NULL,
   kafkaTargetid VARCHAR(100) NOT NULL,
   hdfsTargetid VARCHAR(100) NOT NULL,
   PRIMARY KEY (qualifiedName)
);

CREATE TABLE IF NOT EXISTS Requests(
   processDate DATETIME,
   request VARCHAR(4096) NOT NULL,
   host VARCHAR(100) NOT NULL,
   port VARCHAR(100) NOT NULL,
   dbname VARCHAR(100) NOT NULL,
   dbtable VARCHAR(100) NOT NULL,
   lastStarted VARCHAR(100) NOT NULL,
   lastEnded VARCHAR(100) NOT NULL,
   runFrequency VARCHAR(100) NOT NULL,
   totalRuns INT(11) NOT NULL,
   successRuns INT(11) NOT NULL,
   failureRuns INT(11) NOT NULL,
   currentState VARCHAR(64) NOT NULL,
   exceptions VARCHAR(1024),
   notes VARCHAR(1024), 
   PRIMARY KEY (host, dbname, dbtable)
);

CREATE TABLE IF NOT EXISTS kafkaRequests(
   processDate DATETIME,
   request VARCHAR(4096) NOT NULL,
   cluster VARCHAR(100) NOT NULL,
   topic VARCHAR(100) NOT NULL,
   alias VARCHAR(100) NOT NULL,
   groupName VARCHAR(100) NOT NULL,
   totalRuns INT(11) NOT NULL,
   successRuns INT(11) NOT NULL,
   failureRuns INT(11) NOT NULL,
   currentState VARCHAR(64) NOT NULL,
   exceptions VARCHAR(1024),
   notes VARCHAR(1024), 
   PRIMARY KEY (cluster, topic, alias)
);

CREATE TABLE IF NOT EXISTS Queries(
   id INT NOT NULL AUTO_INCREMENT,
   query_type VARCHAR(8) NOT NULL,
   query_loc VARCHAR(100) NOT NULL,
   output_loc VARCHAR(100) NOT NULL,
   runtype VARCHAR(100) NOT NULL,
   schedule VARCHAR(100) NOT NULL,
   subject VARCHAR(100) NOT NULL
   PRIMARY KEY (id)
);