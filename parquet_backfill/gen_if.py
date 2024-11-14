#!/usr/bin/python

import sys

def main():
	delimiter=','
	name=""
	col_names = list()
	types = list()
	i=0
	for line in sys.stdin:
		line = line.rstrip()
		if (line[0:3] == "def"):
			table = line[line.index('_')+1:line.index('(')]
			if(i == 0):
				print('if (table.equals("'+table+'")){')
			else:
				print('}else if (table.equals("'+table+'")){')
			print('  val df = convert_'+table+'(filename)')
			print('  df.repartition(1).write.mode("overwrite").parquet(newbasefile)')
			i+=1
	print('}')



if __name__=="__main__":
	main()
