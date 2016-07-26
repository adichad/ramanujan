CREATE DATABASE IF NOT EXISTS Ramanujan;
USE Ramanujan;
CREATE TABLE IF NOT EXISTS BookMarks(
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
