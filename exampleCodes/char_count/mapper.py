import sys
filepath = sys.argv[1]
respath = sys.argv[2]
cnt = {}  
with open(filepath) as fp:
	line = fp.readline()
	while line:
		word_list = list(line)
		for ch in word_list:
			if ord(ch) == 10:
				continue;
			if ch in cnt:
				cnt[ch] += 1
			else:
				cnt[ch] = 1
		line = fp.readline()

f = open(respath, "w")
for k,v in cnt.items():
	f.write(k + " " + str(v) + "\n")
f.close()