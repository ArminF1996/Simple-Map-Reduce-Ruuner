import sys
filepath = sys.argv[1]
respath = sys.argv[2]
mydict = dict()  
with open(filepath) as fp:
	line = fp.readline()
	while line:
		mylist = line.split()
		name = mylist[0]
		family = mylist[1]
		city = mylist[2]
		year = mylist[3]
		key = name + "-" + family + "-" + year
		if key not in mydict:
			mydict[key] = set()
		mydict[key].add(city)
		line = fp.readline()

f = open(respath, "w")
for k,v in mydict.items():
	f.write(k)
	for value in set(v):
		f.write(" " + value)
	f.write("\n")
f.close()