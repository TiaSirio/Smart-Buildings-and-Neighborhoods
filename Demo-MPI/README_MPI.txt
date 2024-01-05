The software can read words from multiple file in input; the files could have more than one word with a maximum size of max_size characters (now set to 11).

The path of the files is: "../files/"

To run the program the following commands must be run on the terminal (from the output directory):
1) mpicc ./../src/word_count.c -o word_count
2) mpirun ./word_count