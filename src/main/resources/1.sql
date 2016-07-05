CREATE DATABASE Ramanujan;
USE Ramanujan;
CREATE TABLE BookMarks(
   id INT NOT NULL AUTO_INCREMENT,
   primarykey VARCHAR(100) NOT NULL,
   bookmark VARCHAR(100) NOT NULL,
   PRIMARY KEY ( id )
);

CREATE TABLE Status(
   id INT NOT NULL AUTO_INCREMENT,
   dbname VARCHAR(100) NOT NULL,
   tablename VARCHAR(100) NOT NULL,
   primarykey VARCHAR(100) NOT NULL,
   sourceid VARCHAR(100) NOT NULL,
   kafkaTargetid VARCHAR(100) NOT NULL,
   hdfsTargetid VARCHAR(100) NOT NULL,
   PRIMARY KEY (id)
);

CREATE TABLE Requests(
   processDate DATETIME,
   request VARCHAR(1024) NOT NULL,
   status VARCHAR(40) NOT NULL,
   PRIMARY KEY (request)
);