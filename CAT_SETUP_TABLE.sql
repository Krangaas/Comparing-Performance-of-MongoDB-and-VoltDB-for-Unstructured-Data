CREATE TABLE CAT (
    FILENAME VARCHAR(25) NOT NULL,
    S0 VARCHAR(1000000 BYTES) NOT NULL,
    S1 VARCHAR(1000000 BYTES),
    S2 VARCHAR(97000 BYTES),
    PRIMARY KEY (FILENAME)
);

TRUNCATE TABLE CAT;
PARTITION TABLE CAT ON COLUMN FILENAME;

CREATE PROCEDURE Insert
    PARTITION ON TABLE CAT COLUMN FILENAME
    AS INSERT INTO CAT (FILENAME, S0, S1, S2)
    VALUES (?, ?, ?, ?);

CREATE PROCEDURE Select
    PARTITION ON TABLE CAT COLUMN FILENAME
    AS SELECT FILENAME, S0, S1, S2 FROM CAT WHERE FILENAME = ?;


CREATE PROCEDURE Delete
    PARTITION ON TABLE CAT COLUMN FILENAME
    AS DELETE FROM CAT WHERE FILENAME = ?;
