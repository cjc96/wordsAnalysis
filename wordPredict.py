# -*- coding: utf-8 -*-
import time

def search(word):
	right = len(dict)
	left = 0
	mid = 0
	lastMid = -1
	word = word.decode('utf8')
	while (lastMid != mid):
		lastMid = mid
		mid = (right + left) / 2
		tmp = dict[mid][:dict[mid].index('\t')].decode('utf8')
		if (tmp == word):
			return dict[mid][dict[mid].index('\t') + 1:]
		elif (tmp < word):
			left = mid
		else:
			right = mid
	return ''

# def search(word):
# 	for t in dict:
# 		# print t[:t.index('\t')].decode('utf8')
# 		try:
# 			if (t[:t.index('\t')].decode('utf8') == word.decode('utf8')):
# 				return t[t.index('\t') + 1:]
# 		except e:
# 			print t
# 	return ''

while (1):
	mode = raw_input("请选择进入何种模式：（1、词语预测；2、单字预测）")
	if (mode == '1'):
		file_object = open('dictWords.txt','r')
		line = file_object.readline()
		dict = []
		while line:
			dict.append(line)
			line = file_object.readline()  
		while (1):
			# now = time.time()
			str = raw_input("请输入一个词语：");
			print "预测下一个词语是"
			ans = search(str)
			if (len(ans) == 0):
				print "没有找到结果"
			else:
				print ans.replace(' ','\n')
			# print time.time() - now
	elif (mode == '2'):
		file_object = open('dict.txt','r')
		line = file_object.readline()
		dict = []
		while line:
			dict.append(line)
			line = file_object.readline()  
		while (1):
			str = raw_input("请输入一个汉字：");
			print "预测下一个汉字是"
			ans = search(str)
			if (len(ans) == 0):
				print "没有找到结果"
			else:
				for tmp in ans.split(' '):
					print tmp
	else:
		print "请重新输入"