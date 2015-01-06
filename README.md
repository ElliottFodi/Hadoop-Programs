Itinerary:
List of programs and description of what they do, in the order found in the submission folder.
Each program can be compiled using java. The MapReduce programs need to be jared, while
the HBase programs do not need to be jared. The Hadoop programs should be run in Hadoop’s main 
directory and the HBase programs can be run from any directory.


Hadoop average in reducer:
This program demonstrates a function in the reducer.

Hadoop Basic word count:
This program is basic word count, the hello world of Hadoop

Hadoop multiline read:
This program reads passes two lines as a record instead of the default one line

Hadoop pass array to reducer:
This program passes an array from the mapper to the reducer, in order to pass an array the
ArrayWritable class needs to be extended to specify the type of values stored in the array

Hadoop pass jar:
This program takes a jar file as a command line argument and uses it in the mapper. To do
thisa tool runner must be used. The tool runner is defined in the main and the MapReduce
configuration and other options are set in the tool runners run method

Hadoop pass object to reducer:
This program creates a writable object that can be passed from the mapper to the reducer.
The writable object is composed of writable primitives.

Hadoop reducer output object:
This program emits a regular Java object in the reducer, this is done to prove that the reducer
does not need to emit writable objects. Since the reducer is writing to HDFS, the object
emitted does not need to be writable, it just needs a toString method

Hadoop reducer prints several names on the same line:
This program accepts a records with several attributes per line. The program lists the position
and all of the characters that play in that position, ignoring all other details in the records This
program shows multiple reducers running and writing to HDFS with the results in the form of a
list
