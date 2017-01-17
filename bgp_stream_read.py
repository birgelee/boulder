#!/usr/bin/env python
# remove for production
#stream.add_filter('prefix', '184.164.226.0/23')




#stream.add_interval_filter(1482700017, 1483304817)



from _pybgpstream import BGPStream, BGPRecord, BGPElem
import time


# create a new bgpstream instance
stream = BGPStream()
# create a reusable bgprecord instance
rec = BGPRecord()
elem = None
# configure the stream to retrieve Updates from the route-views listener.
stream.add_filter('collector', 'route-views.sg')
stream.add_filter('record-type','updates') # here we collect updates. This could be changed to ribs to instead acquire periodic snapshots of the RIBs.
# getting updates only from one peer gives us only the perferred route of this peer and no rejected routes.
stream.add_filter('peer-asn', '24482')
# select the time interval to process: 0 means continue to process live updates. - 345600 allows us to process a 4 day backlog of updates
stream.add_interval_filter(int(time.time()) - 345600, 0)

# start the stream
stream.start()

def primeNextElem():
	global elem, rec
	while (not elem):
		if (not stream.get_next_record(rec)):
			return None
		elem = rec.get_next_elem()
	return elem

def nextUpdateExists():
	global elem, rec
	return (elem is not None) or (primeNextElem())

def fetchNextUpdate():
	global elem, rec
	if (elem):
		res = {'time': rec.time, 'prefix': elem.fields['prefix'] if 'prefix' in elem.fields else None, 
		'type': elem.type, 'as-path': elem.fields['as-path'] if 'as-path' in elem.fields else None}
		elem = rec.get_next_elem()
		return res
	else:
		if nextUpdateExists():
			res = {'time': rec.time, 'prefix': elem.fields['prefix'], 'type': elem.type, 'as-path': elem.fields['as-path']}
			elem = rec.get_next_elem()
			return res
		else:
			return None


