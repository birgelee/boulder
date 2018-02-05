#This is the core file for the certificate age study. It calls upon bgp_stream_read and read_certificate_history (requires crtlist2.txt).
from bgp_stream_read import nextUpdateExists, fetchNextUpdate, primeNextElem
from read_certificate_history import getNextCertificate
import MySQLdb as mariadb
import time
import socket
import netaddr


conn = mariadb.connect('boulder_bmysql_1', 'bgp_processor', 'bgp_processor_pass1!', 'boulder_sa_integration', port=3306)
cursor = conn.cursor()

routeAges = []

def processCertificate():
	global currentCRT, cursor, crtTimeStr, crtTime
	
	routeAge = {}
	try:
		ipString = socket.gethostbyname(currentCRT['commonName'])
		for i in range(24,7,-1):
			cidr = netaddr.IPNetwork(str(ipString) +"/" + str(i)).cidr
			cursor.execute("""SELECT timeList FROM bgpPrefixUpdates WHERE prefix='{}'""".format(str(cidr)))
			result = cursor.fetchone()

			#if this network is in the database, store the result
			if result:
				(storedTimeList,) = result
				storedTimeListArray = storedTimeList.split(' ')

				deltaTimeListArray = storedTimeList.split(' ')

				for i in xrange(len(storedTimeListArray)):
					deltaTimeListArray[i] = str(crtTime - int(storedTimeListArray[i]))

				deltaTimeList = ' '.join(deltaTimeListArray)
				routeAge = {"commonName": currentCRT['commonName'], "hopAge": deltaTimeList}
				break

		#if the for loop ends with something other than the break statement the route was not in the database.
		if routeAge == {}:
			routeAge = {"commonName": currentCRT['commonName'], "hopAge": "-1"}
	except socket.gaierror:
		#nx domain case. Too bad we can't handle this.
		routeAge = {"commonName": currentCRT['commonName'], "hopAge": "-2"}
	#it should be noted that -1 and -2 for hop age strings are magic values. -1 Implies that the ip address was resolved but the route was not in the database meaning that it is a route older than the period we populated the database with.
	#-2 indicates that the domain name no longer resolves to an IP. This is an onfortunate case because we have no clue how old the route is.
	#the -2 error also highlights the imperfections of these methods, between when Let's Encrypt actually issued the cert and when we run this script the domain name could eaily have changed.
	#unfortunatly I can not think of any way to handle this at the moment. We would require access to some sort of historical DNS record.
	#DNS Trails seems to have this type of functionality, but it only supports a small number of top level domains so it might not be worth implementing.
	#Also, DNS is location speciffic so to be rigurious we would have to also know what DNS answer Let's Encrypt speciffically got at that time.
	print {"timeOfCertificateIssuing": crtTime, "rateAgeInfo": routeAge}
	routeAges.append(routeAge)

	currentCRT = getNextCertificate()
	if (not currentCRT):
		exit()
	crtTimeStr = currentCRT['timestamp']
	crtTime = time.mktime(time.strptime(crtTimeStr[:-4], "%Y-%m-%d %H:%M:%S"))
	#cursor.execute("""SELECT prefix, asPath, timeList FROM bgpPrefixUpdates WHERE prefix='{}'""".format(prefix))
	#result = cursor.fetchone()
	

printCounter = 0

currentCRT = getNextCertificate()
if (not currentCRT):
		exit()
crtTimeStr = currentCRT['timestamp']
crtTime = time.mktime(time.strptime(crtTimeStr[:-4], "%Y-%m-%d %H:%M:%S"))

while (nextUpdateExists()):
	update = fetchNextUpdate()
	while update['time'] > crtTime:
		processCertificate()

	if printCounter % 1000 == 0:
		print {"bgpUpdateProcessTimeStamp": update['time'], "percentage": (update['time'] - (1487214969 - 2592000.0))/2592000.0}
		printCounter = 1
	else:
		printCounter += 1

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
			newTimeListArray = [str(update['time'])] * len(gvenASPathArray)
			for i in xrange(len(newTimeListArray)):
				if (len(storedTimeListArray) - i - 1 >= 0):
					#print "new time list array length: " + str(len(newTimeListArray)) + ", len(gvenASPathArray) - i - 1: " + str(len(gvenASPathArray) - i - 1)
					#print "len(storedASPathArray): " + str(len(storedASPathArray)) + ", len(storedASPathArray) - i - 1: " + str(len(storedASPathArray) - i - 1)
					#print "newtimelist array access: " + str(newTimeListArray[len(gvenASPathArray) - i - 1])
					#print "stored timelist array acces" + str(storedTimeListArray[len(storedASPathArray) - i - 1])
					newTimeListArray[len(newTimeListArray) - i - 1] = storedTimeListArray[len(storedTimeListArray) - i - 1]
				else:
					break
			newTimeList = ' '.join(newTimeListArray)
			cursor.execute("""INSERT INTO bgpPrefixUpdates (prefix, asPath, timeList) VALUES ('{}', '{}', '{}') 
				ON DUPLICATE KEY UPDATE asPath='{}', timeList='{}'""".format(prefix, asPath, newTimeList, asPath, newTimeList))
			conn.commit()


	elif (update['type'] == 'W'):
		prefix = conn.escape_string(update['prefix'])
		cursor.execute("DELETE FROM bgpPrefixUpdates WHERE prefix='{}'".format(prefix))
		conn.commit()
