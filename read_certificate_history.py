import json


data_file = open('crtlist2.txt')
data = json.load(data_file)

crtIndex = len(data) - 1

def getNextCertificate():
	global crtIndex
	if crtIndex < 0:
		return None
	crtIndex -= 1
	return data[crtIndex + 1]