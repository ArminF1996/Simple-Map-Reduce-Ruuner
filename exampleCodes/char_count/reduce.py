import sys

filepath = sys.argv[1]
respath = sys.argv[2]
cnt = {}  
with open(filepath) as fp:
   line = fp.readline()
   while line:
      tmp = line.split()
      ch = tmp[0]
      num = int(tmp[1])
      if ch in cnt:
         cnt[ch] += num
      else:
         cnt[ch] = num
      line = fp.readline()

f = open(respath, "w")
for k,v in cnt.items():
	f.write(k + " " + str(v) + "\n")
f.close()