CREATE DATABASE Ramanujan;
USE Ramanujan;
CREATE TABLE BookMarks(
   id INT NOT NULL AUTO_INCREMENT,
   primarykey VARCHAR(100) NOT NULL,
   bookmark VARCHAR(100) NOT NULL,
   PRIMARY KEY ( id )
);

CREATE TABLE Status(
   dbname VARCHAR(100) NOT NULL,
   tablename VARCHAR(100) NOT NULL,
   primarykey VARCHAR(100) NOT NULL,
   qualifiedName VARCHAR(256) not NULL,
   sourceid VARCHAR(100) NOT NULL,
   kafkaTargetid VARCHAR(100) NOT NULL,
   hdfsTargetid VARCHAR(100) NOT NULL,
   PRIMARY KEY (qualifiedName)
);

CREATE TABLE Requests(
   processDate DATETIME,
   request VARCHAR(1024) NOT NULL,
   host VARCHAR(100) NOT NULL,
   dbname VARCHAR(100) NOT NULL,
   dbtable VARCHAR(100) NOT NULL,
   status VARCHAR(40) NOT NULL,
   PRIMARY KEY (host, dbname, dbtable)
);