DROP TABLE IF EXISTS `student`;
CREATE TABLE `student` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(25) COLLATE utf8_bin DEFAULT NULL,
  `password` varchar(25) COLLATE utf8_bin DEFAULT NULL,
  `age` int(10) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;



INSERT INTO `student` VALUES ('1', 'shangbaishuyao01', '123456', '18'), ('2', 'shangbaishuyao02', '123', '17'), ('3', 'shangbaishuyao03', '1234', '18'), ('4', 'shangbaishuyao04', '12345', '16');
COMMIT;
