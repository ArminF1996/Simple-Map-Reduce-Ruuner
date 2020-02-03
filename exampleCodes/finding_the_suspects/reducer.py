import sys
filepath = sys.argv[1]
respath = sys.argv[2]
mydict = dict()  
with open(filepath) as fp:
   line = fp.readline()
   while line:
      mylist = line.split()
      key = mylist[0]
      if key not in mydict:
         mydict[key] = set()
      for i in range(1, len(mylist)):
         mydict[key].add(mylist[i])
      line = fp.readline()

f = open(respath, "w")
for k,v in mydict.items():
   if len(v) <= 10:
      continue
   f.write(k)
   for value in set(v):
      f.write(" " + value)
   f.write("\n")
f.close()