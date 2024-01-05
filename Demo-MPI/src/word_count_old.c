#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>

#define max_size 10//11//10//1

// Distributed count of the number of occurrences of each character in a set of files.
//
// For simplicity, the program considers only the 26 (lower case) letters of the english
// alphabet.
//
// The computation takes place in two phases (inspired by the MapReduce programming model).
//
// In the first phase, each process reads one file and computes the number of occurrences
// of each character in that file.
//
// In the second phase, each process becomes responsible for some of the characters and
// aggregates the partial counts for those characters.
//
// The code exemplifies the use of custom datatypes and the use of asynchronous communication.
//
// Possible extensions/improvements:
// 1. Each process can send (asynchronously) all the data it has for other processes
//    before starting to receive.
// 2. The number of processes could be different than the number of files.
// 3. The final result can be collected in a single node.


typedef struct pair_char_pair_t {
    char c[max_size];
    int count;
    //int rank;
} char_int_pair;

int main(int argc, char** argv) {
    //const int num_letters = 26;
    int actualSize = 0;
    const char *files_path = "../files/in";
    int wordFound = 0;
    int first = 1;
    int endedWithNewLine = 0;

    MPI_Init(NULL, NULL);

    // Create the char_int_pair MPI datatype
    char_int_pair pair;
    MPI_Datatype mpi_char_int_pair;
    int struct_len = 2;
    //int struct_len = 3;
    int block_lens[struct_len];
    MPI_Datatype types[struct_len];
    // We need to compute the displacement to be really portable
    // (different compilers might align structures differently)
    MPI_Aint displacements[struct_len];
    MPI_Aint current_displacement = 0;
    // Add one char
    block_lens[0] = max_size;//1;
    types[0] = MPI_CHAR;
    //displacements[0] = (size_t) &(pair.c) - (size_t) &pair;
    displacements[0] = offsetof(char_int_pair, c);
    // Add one int
    block_lens[1] = 1;
    types[1] = MPI_INT;
    //displacements[1] = (size_t) &(pair.count) - (size_t) &pair;
    displacements[1] = offsetof(char_int_pair, count);
    //block_lens[2] = 1;
    //types[2] = MPI_INT;
    //displacements[2] = (size_t) &(pair.count) - (size_t) &pair;
    //displacements[2] = offsetof(char_int_pair, rank);
    // Create and commit the data structure
    MPI_Type_create_struct(struct_len, block_lens, displacements, types, &mpi_char_int_pair);
    MPI_Type_commit(&mpi_char_int_pair);

    int my_rank, world_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);




    int files_path_len;
    char my_file[files_path_len + 2];
    // Getting the name of my file (supports up to 100 processes)
    //if (my_rank == 0) {
    files_path_len = strlen(files_path);
    strcpy(my_file, files_path);
    sprintf(&my_file[files_path_len], "%d", my_rank);
    //}




    //Divide the files for the different processes
    //MPI_Scatter(my_file, files_path_len + 2, MPI_CHAR, my_file, files_path_len + 2, MPI_CHAR, 0, MPI_COMM_WORLD);
    //MPI_Bcast(&my_file, 1, MPI_CHAR, 0, MPI_COMM_WORLD);


    // Phase 1 (read input file and compute the number of occurrences of each character)
    /*char_int_pair *local_count = (char_int_pair *) malloc(sizeof(char_int_pair) * num_letters);
    for (int i = 0; i < num_letters; i++) {
      local_count[i].c[0] = i + 'a';
      local_count[i].count = 0;
    }
    FILE *file = fopen(my_file, "r");
    char c;
    while ((c = fgetc(file)) != EOF) {
        int c_index = (int) c - 'a';
        local_count[c_index].count++;
    }*/

    char_int_pair* local_count;

    FILE *file = fopen(my_file, "r");
    char c;
    char word[max_size];
    int k = 0;
    size_t len;
    int r = 0;
    char* lineChar;
    //printf("%s", my_file);
    while ((c = fgetc(file)) != EOF) {
        //Get the word
        if (c != '\n' && c != ' '){
            word[k] = c;
            k++;
            endedWithNewLine = 0;
        }
        //Assign the word if we find newLine or space
        if (c == '\n' || c == ' '){
            //Assign word and clear it

            //Cycle below doesn't solve the problem for \n
            while(word[r] != '\0') {
                if(word[r] == '\n' || word[r] == ' ')
                {
                    word[r] = '\0';
                }
                r++;
            }
            r = 0;
            if (first == 1) {
                first = 0;
                actualSize++;
                local_count = (char_int_pair *) malloc(sizeof(char_int_pair) * actualSize);

                len = strlen(word);
                memcpy(local_count[0].c, word, len);
                local_count[0].c[len] = '\0';

                local_count[0].count = 1;
                wordFound = 1;
            }
            if (wordFound == 0) {
                for (int i = 0; i < actualSize; i++) {
                    if (strcmp(local_count[i].c, word) == 0) {
                        local_count[i].count++;
                        wordFound = 1;
                        //break;
                    }
                }
            }
            if (wordFound == 0) {
                actualSize++;
                local_count = (char_int_pair *) realloc(local_count, sizeof(char_int_pair) * actualSize);

                len = strlen(word);
                memcpy(local_count[actualSize - 1].c, word, len);
                local_count[actualSize - 1].c[len] = '\0';

                local_count[actualSize - 1].count = 1;
                //printf("%s", local_count[actualSize - 1].c);
            }
            memset(word, 0, sizeof(word));
            wordFound = 0;
            k = 0;
            endedWithNewLine = 1;
        }
    }

    //Assign the word if we reach the EOF
    if (endedWithNewLine == 0) {

        //Cycle below doesn't solve the problem for \n
        while(word[r] != '\0')
        {
            if(word[r] == '\n' || word[r] == ' ')
            {
                word[r]='\0';
            }
            r++;
        }
        r = 0;
        if (first == 1) {
            first = 0;
            //It could be good just this
            actualSize++;
            local_count = (char_int_pair *) malloc(sizeof(char_int_pair) * actualSize);

            len = strlen(word);
            memcpy(local_count[0].c, word, len);
            local_count[0].c[len] = '\0';

            //printf("%s", local_count[0].c);
            local_count[0].count = 1;
            wordFound = 1;

            //local_count[0].rank = my_rank;
            //printf("%s   ->   %d   ->   myRank = %d\n", local_count[0].c, local_count[0].count, my_rank);
        }
        if (wordFound == 0) {
            for (int i = 0; i < actualSize; i++) {
                if (strcmp(local_count[i].c, word) == 0) {
                    local_count[i].count++;
                    wordFound = 1;
                    //break;
                }
                //printf("%s   ->   %d   ->   myRank = %d\n", local_count[0].c, local_count[0].count, my_rank);
            }
        }
        if (wordFound == 0) {
            actualSize++;
            local_count = (char_int_pair *) realloc(local_count, sizeof(char_int_pair) * actualSize);

            len = strlen(word);
            memcpy(local_count[actualSize - 1].c, word, len);
            local_count[actualSize - 1].c[len] = '\0';

            local_count[actualSize - 1].count = 1;
        }
        memset(word, 0, sizeof(word));
    }
    fclose(file);


    //printf("%s   ->   %d   ->   myRank = %d\n", local_count[0].c, local_count[0].count, local_count[0].rank);


    //Gather, collecting the different word and create the structure
    char_int_pair* gather_buffer = NULL;
    if (my_rank == 0) {
        //Penso che non prenda le altre parole perché devo allocare lo spazio (conoscendo le parole per testo peró) totale che poi la gather si gestisce (quindi da fixare anche la gather)
        gather_buffer = (char_int_pair *) malloc(sizeof(char_int_pair) * world_size);
    }

    MPI_Gather(local_count, 1, mpi_char_int_pair, gather_buffer, 1, mpi_char_int_pair, 0, MPI_COMM_WORLD);

    int found = 0;
    int first_round = 1;

    if (my_rank == 0) {
        for (int j = 0; j < world_size; j++){
            /*for (int i = 0; i < world_size; i++) {
                //printf("%s   ->   %s   ->   %d\n", local_count[j].c, gather_buffer[i].c, local_count[j].count);
                gather_buffer[i].c[max_size] = '\0';
                if (strcmp(local_count[j].c, gather_buffer[i].c) == 0) {
                    local_count[j].count += gather_buffer[i].count;
                    break;
                } else {
                    //Otherwise realloc new space
                    actualSize++;
                    local_count = (char_int_pair *) realloc(local_count, sizeof(char_int_pair) * actualSize);
                    memcpy(local_count[actualSize - 1].c, gather_buffer[i].c, sizeof(gather_buffer[i].c));
                    local_count[actualSize - 1].count += gather_buffer[i].count;
                    //printf("%s   ->   %s   ->   %d\n", local_count[j].c, gather_buffer[i].c, gather_buffer[j].count);
                    //for (int j = 0; j < actualSize; j++) {
                    //    printf("%d, %s, %d\n", j, local_count[j].c, local_count[j].count);
                    //}
                    //printf("%s\n", rec_buffer.c);
                    //printf("%s\n", local_count[actualSize - 1].c);
                    //printf("size -> %d\n", actualSize);
                    break;
                }
                //printf("%s -> %d", gather_buffer[i].c, gather_buffer[i].count);
            }*/

            if (first_round == 0){
                for (int i = 0; i < actualSize; i++) {
                    /*lineChar = strchr(gather_buffer[j].c, '\n');
                    if (lineChar != NULL){
                        printf("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
                    }*/
                    gather_buffer[j].c[max_size] = '\0';
                    if (strcmp(local_count[i].c, gather_buffer[j].c) == 0 /* && gather_buffer[j].rank != 0*/) {
                        local_count[i].count += gather_buffer[j].count;
                        found = 1;
                        break;
                    }
                }
                if (found == 0) {
                    actualSize++;
                    local_count = (char_int_pair *) realloc(local_count, sizeof(char_int_pair) * actualSize);
                    memcpy(local_count[actualSize - 1].c, gather_buffer[j].c, sizeof(gather_buffer[j].c));
                    local_count[actualSize - 1].count += gather_buffer[j].count;
                }
                found = 0;
                /*for (int s = 0; s < actualSize; s++) {
                    printf("%d, %s, %d\n", s, local_count[s].c, local_count[s].count);
                }*/
            }
            first_round = 0;
        }
    }



    // Sending and receiving each letter
    /*MPI_Request req;
    for (int i = 0; i < actualSize; i++) {
        int receiver = i % world_size;
        // I am the receiver
        if (receiver == my_rank) {
            // I receive one message from any other process
            for (int p = 0; p < world_size - 1; p++) {
                char_int_pair rec_buffer;
                MPI_Recv(&rec_buffer, 1, mpi_char_int_pair, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                //local_count[i].c[max_size] = '\0';
                //rec_buffer.c[max_size] = '\0';
                //rec_buffer.c[max_size] = '\0';
                //printf("%s   ->   %s\n", local_count[i].c, rec_buffer.c);
                //If the same, add to the current count
                if (strcmp(local_count[i].c, rec_buffer.c) == 0) {
                    local_count[i].count += rec_buffer.count;
                } else {
                    //Otherwise realloc new space
                    actualSize++;
                    local_count = (char_int_pair *) realloc(local_count, sizeof(char_int_pair) * actualSize);
                    memcpy(local_count[actualSize - 1].c, rec_buffer.c, sizeof(rec_buffer.c));
                    local_count[actualSize - 1].count += rec_buffer.count;
                    //for (int j = 0; j < actualSize; j++) {
                    //    printf("%d, %s, %d\n", j, local_count[j].c, local_count[j].count);
                    //}
                    //printf("%s\n", rec_buffer.c);
                    //printf("%s\n", local_count[actualSize - 1].c);
                    //printf("size -> %d\n", actualSize);
                }
            }
        }
            // I am a sender: I can send asynchronously
        else {
            MPI_Isend(&local_count[i], 1, mpi_char_int_pair, receiver, 0, MPI_COMM_WORLD, &req);
        }
    }*/



    // When done, everybody prints its own results
    /*for (int i = 0; i < actualSize; i++) {
        //printf("%d", actualSize);
        int owner = i % world_size;
        if (owner == my_rank) {
            //printf("%c -> %d\n", local_count[i].c[0], local_count[i].count);
            printf("%s -> %d\n", local_count[i].c, local_count[i].count);
        }
    }*/

    if (my_rank == 0) {
        for (int i = 0; i < actualSize; i++) {
            printf("%s -> %d\n", local_count[i].c, local_count[i].count);
            //Usando la printf sotto, sembra che il \n faccia baggare la count e non la stringa (tipo una buffer overflow nella struct dove sembra si inserisca il \n nell'elemento sotto della struct)
            //printf("Questa é la parola: %s -> %d\n", local_count[i].c, local_count[i].count);
        }
    }

    free(gather_buffer);
    free(local_count);
    MPI_Type_free(&mpi_char_int_pair);
    MPI_Finalize();
}