-- auto Generated on 2021-04-01
-- DROP TABLE IF EXISTS person;
CREATE TABLE person(
	id INT (11) NOT NULL AUTO_INCREMENT COMMENT 'id',
	pid VARCHAR (50) NOT NULL DEFAULT '' COMMENT 'pid',
	`number` VARCHAR (50) NOT NULL DEFAULT '' COMMENT 'number',
	PRIMARY KEY (id)
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'person';
