from bgp_stream_read import nextUpdateExists, fetchNextUpdate, primeNextElem
import MySQLdb as mariadb


conn = mariadb.connect('boulder_bmysql_1', 'bgp_processor', 'bgp_processor_pass1!', 'boulder_sa_integration', port=3306)
cursor = conn.cursor()



while (nextUpdateExists()):
	update = fetchNextUpdate()
	if (update['type'] == 'A'):

		asPath = conn.escape_string(update['as-path'])
		asPathLength = asPath.count(' ') + 1
		prefix = conn.escape_string(update['prefix'])
		cursor.execute("""SELECT prefix, asPath, timeList FROM bgpPrefixUpdates WHERE prefix='{}'""".format(prefix))
		result = cursor.fetchone()


		if (result == None):
			timeList = ' '.join([str(update['time'])] * asPathLength)
			cursor.execute("""INSERT INTO bgpPrefixUpdates (prefix, asPath, timeList) VALUES ('{}', '{}', '{}') 
				ON DUPLICATE KEY UPDATE asPath='{}', timeList='{}'""".format(prefix, asPath, timeList, asPath, timeList))
			conn.commit()

		else:
			(_, storedASPath, storedTimeList) = result
			if (asPath == storedASPath):
				continue

			storedTimeListArray = storedTimeList.split(' ')
			storedASPathArray = storedASPath.split(' ')

			gvenASPathArray = asPath.split(' ')
			newTimeListArray = [str(update['time'])] * asPathLength
			for i in xrange(len(gvenASPathArray)):
				if (len(storedASPathArray) - i - 1 >= 0):
					newTimeListArray[len(gvenASPathArray) - i - 1] = storedTimeListArray[len(storedASPathArray) - i - 1]
					print "test"
				else:
					break
			newTimeList = ' '.join(newTimeListArray)
			print "**************"
			cursor.execute("""INSERT INTO bgpPrefixUpdates (prefix, asPath, timeList) VALUES ('{}', '{}', '{}') 
				ON DUPLICATE KEY UPDATE asPath='{}', timeList='{}'""".format(prefix, asPath, newTimeList, asPath, newTimeList))
			conn.commit()
			print """INSERT INTO bgpPrefixUpdates (prefix, asPath, timeList) VALUES ('{}', '{}', '{}') 
				ON DUPLICATE KEY UPDATE asPath='{}', timeList='{}'""".format(prefix, asPath, newTimeList, asPath, newTimeList)


	elif (update['type'] == 'W'):
		prefix = conn.escape_string(update['prefix'])
		cursor.execute("DELETE FROM bgpPrefixUpdates WHERE prefix='{}'".format(prefix))
		conn.commit()
		#print("DELETE FROM bgpPrefixUpdates WHERE prefix='{}'".format(prefix))
