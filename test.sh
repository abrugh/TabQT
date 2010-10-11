#!/bin/bash

#cat *txt | sed -f split.sed | LC_COLLATE=C sort | while read line; do echo 1 $line; done >

#running script on sorted data
START1=`date +%s`
cat tmp/words.sorted.final | python tqt.py  -i -f "$" -a add -k 2 -v 1 -F' ' -s > tmp/sorted 2>/dev/null
END1=`date +%s`

echo "sorted test took $((END1-START1)) seconds"

#running script on unsorted data
START2=`date +%s`
cat tmp/words.unsorted.final | python tqt.py  -i -f "$" -a add -k 2 -v 1 -F' ' > tmp/unsorted  2>/dev/null
END2=`date +%s`

echo "unsorted test took $((END2-START2)) seconds"

FAIL=`diff tmp/sorted tmp/unsorted`

if [ -n "$FAIL" ]; then 
	echo "Something failed"
	echo $FAIL
else
	echo "Test Passed"

fi
