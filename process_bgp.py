from bgp_stream_read import nextUpdateExists, fetchNextUpdate, primeNextElem
import MySQLdb as mariadb


conn = mariadb.connect('boulder_bmysql_1', 'bgp_processor', 'bgp_processor_pass1!', 'boulder_sa_integration', port=3306)
cursor = conn.cursor()



while (nextUpdateExists()):
	update = fetchNextUpdate()
	if (update['type'] == 'A'):
		asPath = conn.escape_string(update['as-path'])
		asPathLength = asPath.count(' ') + 1
		timeList = ' '.join([str(update['time'])] * asPathLength)
		prefix = conn.escape_string(update['prefix'])
		cursor.execute("""INSERT INTO bgpPrefixUpdates (prefix, asPath, timeList) VALUES ('{}', '{}', '{}') 
			ON DUPLICATE KEY UPDATE asPath='{}', timeList='{}'""".format(prefix, asPath, timeList, asPath, timeList))
		conn.commit()
		#print("INSERT INTO bgpPrefixUpdates (prefix, asPath, timeList) VALUES ('{}', '{}', '{}') ON DUPLICATE KEY UPDATE asPath='{}', timeList='{}'".format(prefix, asPath, timeList, asPath, timeList))
	elif (update['type'] == 'W'):
		prefix = conn.escape_string(update['prefix'])
		cursor.execute("DELETE FROM bgpPrefixUpdates WHERE prefix='{}'".format(prefix))
		conn.commit()
		#print("DELETE FROM bgpPrefixUpdates WHERE prefix='{}'".format(prefix))
