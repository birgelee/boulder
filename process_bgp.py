#This file is run continually by the CA to keep the bgp database up to date. Requres bgp_stream_read.
from bgp_stream_read import nextUpdateExists, fetchNextUpdate, primeNextElem
import MySQLdb as mariadb


conn = mariadb.connect('boulder_bmysql_1', 'bgp_processor', 'bgp_processor_pass1!', 'boulder_sa_integration', port=3306)
cursor = conn.cursor()



while (nextUpdateExists()):
	update = fetchNextUpdate()
	print "Processing update with time: {0}".format(update['time'])
	if update['type'] == 'A' or update['type'] == 'R':

		asPath = conn.escape_string(update['as-path'])
		asPathLength = asPath.count(' ') + 1
		prefix = conn.escape_string(update['prefix'])
		cursor.execute("""SELECT asPath, timeList FROM bgpPrefixUpdates WHERE prefix='{0}'""".format(prefix))
		result = cursor.fetchone()

		if (result == None):
			timeList = ' '.join([str(update['time'])] * asPathLength)
			cursor.execute("""INSERT INTO bgpPrefixUpdates (prefix, asPath, timeList, updateTime) VALUES ('{0}', '{1}', '{2}', {3}) 
							ON DUPLICATE KEY UPDATE asPath='{4}', timeList='{5}', 
							updateTime={6}""".format(prefix, asPath, timeList, int(update['time']), asPath, timeList, int(update['time'])))
			conn.commit()

		else:
			(storedASPath, storedTimeList) = result
			if storedASPath == None:
				# This is the case where the prefix had been withdrawn.
				timeList = ' '.join([str(update['time'])] * asPathLength)
				cursor.execute("""UPDATE bgpPrefixUpdates SET asPath='{0}', timeList='{1}', updateTime={2} WHERE prefix='{3}'""".format(asPath, timeList, int(update['time']), prefix))
				conn.commit()
			else:
				if asPath == storedASPath:
					continue

			storedTimeListArray = storedTimeList.split(' ')
			storedASPathArray = storedASPath.split(' ')

			gvenASPathArray = asPath.split(' ')
			newTimeListArray = [str(update['time'])] * asPathLength
			for i in xrange(len(gvenASPathArray)):
				if (len(storedASPathArray) - i - 1 >= 0):
					newTimeListArray[len(gvenASPathArray) - i - 1] = storedTimeListArray[len(storedASPathArray) - i - 1]
				else:
					break
			newTimeList = ' '.join(newTimeListArray)
			#print "**************"
			cursor.execute("""INSERT INTO bgpPrefixUpdates (prefix, asPath, timeList, updateTime) VALUES ('{0}', '{1}', '{2}', {3}) 
							ON DUPLICATE KEY UPDATE asPath='{4}', timeList='{5}', 
							updateTime={6}""".format(prefix, asPath, newTimeList, int(update['time']), asPath, newTimeList, int(update['time'])))
			conn.commit()
			#print """INSERT INTO bgpPrefixUpdates (prefix, asPath, timeList) VALUES ('{}', '{}', '{}') 
			#	ON DUPLICATE KEY UPDATE asPath='{}', timeList='{}'""".format(prefix, asPath, newTimeList, asPath, newTimeList)

	elif (update['type'] == 'W'):
		prefix = conn.escape_string(update['prefix'])

		cursor.execute("""SELECT prefix, asPath, timeList, previousASPath, addedTime FROM bgpPrefixUpdates WHERE prefix='{0}'""".format(prefix))
		result = cursor.fetchone()
		if result == None:
			# Since we are not seeding this is not that unusual.
			print "Route withdrawn from prefix that was not in DB. Prefix: {0}".format(prefix)
			#print "Route withdrawn from prefix that was not in DB. This is likely an error caused by improper seeding. Prefix: {0}".format(prefix)
		else:
			(_, storedASPath, storedTimeList, previousASPath, storedAddedTime) = result
			
			if storedASPath == None:
				print "Prefix withdrawn twice. Prefix: {0}".format(prefix)
			else:
				updateTime = int(update['time'])
				addedTime = updateTime - int(storedTimeList.split(' ')[0])
				if storedASPath == previousASPath:
					addedTime += storedAddedTime
				cursor.execute("""UPDATE bgpPrefixUpdates SET asPath=NULL, timeList=NULL, previousASPath='{0}', addedTime={1} WHERE prefix='{2}'""".format(storedASPath, addedTime, prefix))
				conn.commit()







		cursor.execute("DELETE FROM bgpPrefixUpdates WHERE prefix='{}'".format(prefix))
		conn.commit()
		#print("DELETE FROM bgpPrefixUpdates WHERE prefix='{}'".format(prefix))
