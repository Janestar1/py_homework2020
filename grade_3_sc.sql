CREATE TABLE IF NOT EXISTS sc  (
    sn         INTEGER,     --序号
    no         VARCHAR(10), --选课号
    stu_sn     INTEGER,     --学号
    cou_sn     INTEGER,     -- 课程序号
    state      TEXT,

    PRIMARY KEY(sn)
);

CREATE SEQUENCE seq_sc_sn 
    START 10000 INCREMENT 1 OWNED BY sc.sn;
ALTER TABLE sc ALTER sn 
    SET DEFAULT nextval('seq_sc_sn');

CREATE UNIQUE INDEX idx_ssc_no ON sc(no);

ALTER TABLE sc 
    ADD CONSTRAINT stu_sn_fk FOREIGN KEY (stu_sn) REFERENCES student(sn);
ALTER TABLE sc
    ADD CONSTRAINT cou_sn_fk FOREIGN KEY (cou_sn) REFERENCES course(sn);