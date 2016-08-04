CREATE TABLE DEPARTMENT (ID CHAR(32) PRIMARY KEY, TYPE INT NOT NULL);
CREATE TABLE MEMBER (ID CHAR(32) PRIMARY KEY, AGE INT);
CREATE TABLE TREE(NODE_ID CHAR(32), NODE_TYPE INT, PARENT_ID CHAR(32), PARENT_TYPE INT, FOREIGN KEY (PARENT_ID) REFERENCES DEPARTMENT(ID));
CREATE TABLE PATH(NODE_ID CHAR(32), NODE_TYPE INT, PARENT_ID CHAR(32), PARENT_TYPE INT, FOREIGN KEY (NODE_ID) REFERENCES DEPARTMENT(ID) ,FOREIGN KEY (PARENT_ID) REFERENCES DEPARTMENT(ID));