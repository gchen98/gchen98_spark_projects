#!/usr/bin/python

import sys

def main():
	delimiter=','
	name=""
	col_names = list()
	types = list()
	for line in sys.stdin:
		line = line.rstrip()
		if(len(line) == 0):
			if(len(col_names)>0):
				sys.stdout.write('case class class_'+name+'(')
				for i in range(0,len(types)):
					if(i>0):
						sys.stdout.write(',')
					sys.stdout.write(col_names[i]+':'+types[i])
				print(')')
				print("def convert_"+name+"(inpath:String):Dataset[class_"+name+"]={")
				print('  val lines = spark.read.format("text").option("delimiter","\\t").load(inpath)')
				print('  val df = lines.map(line=>{')
				print('    val oldfields = line(0).toString.split("\\t")')
				print('    var fields = oldfields')
				print('    if(oldfields.length<'+str(len(types))+') fields = oldfields.padTo('+str(len(types))+',"")')
				sys.stdout.write('    class_'+name+'(')
				for i in range(0,len(types)):
					if(i>0):
						sys.stdout.write(',')
					if(types[i] == 'Boolean'):
						sys.stdout.write('str2bool(')
					if(types[i] == 'Integer'):
						sys.stdout.write('str2int(')
					sys.stdout.write('fields('+str(i)+')')
					if(types[i] == 'Boolean' or types[i] == 'Integer'):
						sys.stdout.write(')')
				sys.stdout.write(')')
				print('})')
				print('  df')
				print('}')
#				for i in range(0,len(types)):
#					if(i>0):
#						sys.stdout.write(',')
#					sys.stdout.write(col_name+':'+types[i])
#					if(types[i] == 'Boolean'):
#						sys.stdout.write('str2bool(')
#					sys.stdout.write('columns('+str(i)+')')
#					if(types[i] == 'Boolean'):
#						sys.stdout.write(')')
#					if(types[i] == 'Integer'):
#						sys.stdout.write('.toInt')
#				sys.stdout.write(')).toDF(')
#				for i in range(0,len(col_names)):
#					col_name = col_names[i]
#					if(i>0):
#						sys.stdout.write(',')
#					if(col_name[0] == '"'):
#						sys.stdout.write(col_name)
#					else:
#						sys.stdout.write('"'+col_name+'"')
#				print(")\n  df\n}")
			col_names = list()
			types = list()

		tokens = line.split(delimiter)
		if(len(tokens)==1):
			name = tokens[0]
		else:
			for token in tokens:
				token = token.strip()
				strlen = len(token)
				if(token[0:6] == "Struct"):
					colname = token[token.index("(")+1:]
					col_names.append(colname)
				if(strlen>5 and token[strlen-4:strlen]=="Type"):
					scalatype = token[0:strlen-4]
					if(scalatype == 'Binary'):
						scalatype = 'String'
					types.append(scalatype)

	print(col_names)


if __name__=="__main__":
	main()
