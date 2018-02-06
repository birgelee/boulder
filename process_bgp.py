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
				# We are working from the end of these arrays and they both have different length. Thus (since the index counts from the beginning) we must use 2 separate indexes for the old and new AS path arrays.
				# This old array index can be less than 0 in the case where the new AS path is shorter. Once the old array index goes blow 0 it is an exit condition which is checked for in the if statement below.
				asPathIndexOldArray = len(storedASPathArray) - i - 1

				# The new index cannot be less than 0 because of the parameters of the loop.
				asPathIndexNewArray = len(gvenASPathArray) - i - 1

				# In addition to the less than 0 exit condition we need to check to make sure the ASes actually match between the two updates.
				if asPathIndexOldArray >= 0 and gvenASPathArray[asPathIndexNewArray] == storedASPathArray[asPathIndexOldArray]:
					newTimeListArray[asPathIndexNewArray] = storedTimeListArray[asPathIndexOldArray]
				else:
					break
			newTimeList = ' '.join(newTimeListArray)
			#print "**************"
			cursor.execute("""UPDATE bgpPrefixUpdates SET asPath='{0}', timeList='{1}', updateTime={2} WHERE prefix='{3}'""".format(asPath, newTimeList, int(update['time']), prefix))
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
