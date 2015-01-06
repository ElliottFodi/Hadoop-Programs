{\rtf1\ansi\ansicpg1252\cocoartf1265\cocoasubrtf210
{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
\margl1440\margr1440\vieww10800\viewh8400\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural

\f0\fs24 \cf0 READ ME HADOOP MAP REDUCE\
\
Compile:\
When compiling include the JSON library and Hadoop libraries in the class path\
command: javac *.java\
\
Jar:\
command: jar -cvf riotMapper.jar *.class\
\
Run:\
This run command is issued in the Hadoop home directory and accepts the JSON jar as an argument.The JSON jar resides on the normal file system.\
\
riotMapper.jar: is the name of the jar file containing the map reduce program\
\
riotMRMain: is the name of the class congaing the main\
\
-libjars: is the command line argument to accept external jar files to be used in the map reuse program\
\
/home/elliott/Libraries/json-simple-1.1.1.jar: is the full path to the JSON jar \
\
riotInput: is the input directory in HDFS which contains the files the map reduce program\
\
riotOutput: is the output directory in HDFS that the map reduce program will create\
  \
command: bin/hadoop jar riotMapper.jar riotMRMain -libjars /home/elliott/Libraries/json-simple-1.1.1.jar riotInput riotOutput\
\
This program expects Riot match data in the form of one match in JSON format per file. It then performs the map reduce on all the matches and prints the output to the specified directory. \
 }