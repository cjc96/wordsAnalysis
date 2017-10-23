#!/bin/sh
X="`date "+%Y%m%d%H%M%S"`"
hadoop jar smoothing.jar hello.Pinyin /users/rocks1/news_sohusite_xml.dat /users/rocks3/14307130003/out${X} $@
hadoop fs -get /users/rocks3/14307130003/out${X}/part-r-00000 .
python smoothing.py $@
