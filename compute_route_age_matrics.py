import json


data_file = open('route-ages.out')
data = json.load(data_file)
routeAgesByPositionFromEnd = []
overallRouteAges = []

domainNotFoundCount = 0
routeNotInDatabaseCount = 0

for routeInfo in data:
	deltaTimeArray = routeInfo['rateAgeInfo']['hopAge'].split(' ')
	deltaTimeArray = map(float,deltaTimeArray)

	#validate that the time list is increasing as expected
	lastIndex = 0
	for index in deltaTimeArray:
		if index == -1:
			lastIndex = -1
			break
		if index == -2:
			lastIndex = -2
			break
		index = float(index)
		if index < lastIndex:
			lastIndex = -7
			break
		lastIndex = index

	#print special messages for cases not handled
	#-7 is an erronious case that should only occur if somehow the imputs to this script are currupt
	#-2 and -1 are perfectly valid cases that simply can't be included in this metric
	if lastIndex == -7:
		print "bad time list for: " + routeInfo['rateAgeInfo']['commonName']
		continue
	if lastIndex == -2:
		domainNotFoundCount += 1
		continue
	if lastIndex == -1:
		routeNotInDatabaseCount += 1
		continue
	overallRouteAges.append(deltaTimeArray[0])
	#add data to index speciffic array
	while (len(routeAgesByPositionFromEnd) < len(deltaTimeArray)):
		routeAgesByPositionFromEnd.append([])
	for i in xrange(len(deltaTimeArray)):
		routeAgesByPositionFromEnd[i].append(float(deltaTimeArray[len(deltaTimeArray) - i - 1]))

print "{:8}|{:8}|{:13}|{:13}|{:13}|{:13}|{:13}|{:13}|{:13}".format("hop","n", "[0]", "1/1000", "1/100", "5/100", "median", "95%", "[len]")


for i in xrange(len(routeAgesByPositionFromEnd)):
	routeAgesByPositionFromEnd[i] = sorted(routeAgesByPositionFromEnd[i])
	times = routeAgesByPositionFromEnd[i]
	length = len(times)
	print "{:8}|{:8}|{:13}|{:13}|{:13}|{:13}|{:13}|{:13}|{:13}".format("hop=" + str(i),"n=" + str(len(routeAgesByPositionFromEnd[i])), times[0], times[int(.001 * length)], times[int(.01 * length)], times[int(.05 * length)], times[int(.5 * length)], times[int(.95 * length)], times[length - 1])

sortedRouteAges = sorted(overallRouteAges)
routeCount = len(sortedRouteAges)
print "route count: " + str(routeCount)
print "domain not found count: " + str(domainNotFoundCount)
print "route not in database: " + str(routeNotInDatabaseCount)
print "1/1000 routes " + str(sortedRouteAges[int(.001 * routeCount)])
print "1/100 routes " + str(sortedRouteAges[int(.01 * routeCount)])
print "5/100 routes " + str(sortedRouteAges[int(.05 * routeCount)])
print "route median update rate " + str(sortedRouteAges[int(.5 * routeCount)])
print "%95 update rate " + str(sortedRouteAges[int(.95 * routeCount)])
print "average route age " + str(sum(sortedRouteAges) / float(len(sortedRouteAges)))


