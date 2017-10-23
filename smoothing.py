# -*- coding: utf-8 -*-
import sys
file_object = open('part-r-00000','r')
line = file_object.readline()
status = []
total = 0
zeroCount = 0
targetCount = 0
while line:
	status.append(line)
	num = int(line[4:-1])
	total += num
	if (num == 1):
		zeroCount += 1
	if (line[:3] == sys.argv[2]):
		targetCount = num
	line = file_object.readline()
print "概率P(W1|W2W3)的概率为:",
if (targetCount == 0):
	print zeroCount * 1.0 / total / 3000, "Good Turing Discount处理"
	print 1.0 / total, "Add-One 处理"
	print zeroCount * 1.0 / total / 3000 * 0.7 + 0.3 / total, "interpolation处理"
else :
	print targetCount * 1.0 / total