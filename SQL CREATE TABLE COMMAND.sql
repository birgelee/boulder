#SQL CREATE TABLE COMMAND

CREATE USER IF NOT EXISTS 'bgp_processor'@'%' IDENTIFIED BY 'bgp_processor_pass1!';
GRANT ALL PRIVILEGES ON *.* TO 'bgp_processor'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;

CREATE USER IF NOT EXISTS 'va'@'%' IDENTIFIED BY 'vauserpass';
GRANT ALL PRIVILEGES ON *.* TO 'va'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;

SET @@GLOBAL.wait_timeout=31536000;

use boulder_sa_integration
CREATE TABLE IF NOT EXISTS bgpPrefixUpdates
(
prefix varchar(255) NOT NULL PRIMARY KEY,
asPath varchar(1000),
timeList varchar(1000),
updateTime int,
previousASPath varchar(1000),
addedTime int
);