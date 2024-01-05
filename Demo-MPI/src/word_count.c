#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>

#define max_size 11

typedef struct pair_char_pair_t {
    char c[max_size];
    int count;
} char_int_pair;

int main(int argc, char** argv) {
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
    int block_lens[struct_len];
    MPI_Datatype types[struct_len];
    // We need to compute the displacement to be really portable
    // (different compilers might align structures differently)
    MPI_Aint displacements[struct_len];
    MPI_Aint current_displacement = 0;
    //Add string
    block_lens[0] = max_size;
    types[0] = MPI_CHAR;
    displacements[0] = (size_t) &(pair.c) - (size_t) &pair;
    //Add the count
    block_lens[1] = 1;
    types[1] = MPI_INT;
    displacements[1] = (size_t) &(pair.count) - (size_t) &pair;
    //Create and commit the data structure
    MPI_Type_create_struct(struct_len, block_lens, displacements, types, &mpi_char_int_pair);
    MPI_Type_commit(&mpi_char_int_pair);

    int my_rank, world_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);



    int files_path_len;
    char my_file[files_path_len + 2];
    files_path_len = strlen(files_path);
    strcpy(my_file, files_path);
    sprintf(&my_file[files_path_len], "%d", my_rank);

    char_int_pair* local_count;

    FILE *file = fopen(my_file, "r");
    char c;
    char word[max_size];
    int k = 0;
    size_t len;
    int r = 0;
    char* lineChar;
    char test;
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
                    break;
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
                break;
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

            local_count[0].count = 1;
            wordFound = 1;
        }
        if (wordFound == 0) {
            for (int i = 0; i < actualSize; i++) {
                if (strcmp(local_count[i].c, word) == 0) {
                    local_count[i].count++;
                    wordFound = 1;
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
        }
        memset(word, 0, sizeof(word));
    }
    fclose(file);


    //Here I gather the sizes
    int* gather_length = NULL;
    int sum = 0;
    int max_value = 0;

    gather_length = (int *) malloc(sizeof(int) * world_size);

    const int size[] = {1,1,1,1};
    const int displacementSize[] = {0,1,2,3};
    int gatherDisplacement[world_size];

    MPI_Gatherv(&actualSize, 1, MPI_INT, gather_length, size, displacementSize, MPI_INT, 0, MPI_COMM_WORLD);

    if (my_rank == 0) {
        gatherDisplacement[0] = 0;
        for (int l = 1; l < world_size; l++){
            if (max_value < gather_length[l]) {
                max_value = gather_length[l];
            }
            gatherDisplacement[l] = sum + actualSize;
            sum += gather_length[l];
        }
        sum += actualSize;
    }


    int found = 0;
    char_int_pair* gather_buffer = NULL;

    //Gather, collecting the different words and create the structure
    if (my_rank == 0) {
        gather_buffer = (char_int_pair *) realloc(gather_buffer, sizeof(char_int_pair) * sum);
    }

    MPI_Gatherv(local_count, actualSize, mpi_char_int_pair, gather_buffer, gather_length, gatherDisplacement, mpi_char_int_pair, 0, MPI_COMM_WORLD);

    if (my_rank == 0) {
        int fixed_size = actualSize;
        //To skip all the elements of the with rank 0
        for (int j = fixed_size; j < sum; j++){
            for (int i = 0; i < actualSize; i++) {
                len = strlen(gather_buffer[j].c);
                memcpy(word, gather_buffer[j].c, len);
                word[len] = '\0';

                if (strcmp(local_count[i].c, word) == 0) {
                    local_count[i].count += gather_buffer[j].count;
                    found = 1;
                    break;
                }
            }
            if (found == 0) {
                actualSize++;
                local_count = (char_int_pair *) realloc(local_count, sizeof(char_int_pair) * actualSize);

                len = strlen(word);
                memcpy(local_count[actualSize - 1].c, word, len);
                local_count[actualSize - 1].c[len] = '\0';

                local_count[actualSize - 1].count += gather_buffer[j].count;
            }
            memset(word, 0, sizeof(word));
            found = 0;
        }
    }

    if (my_rank == 0) {
        for (int i = 0; i < actualSize; i++) {
            printf("%s -> %d\n", local_count[i].c, local_count[i].count);
            //Usando la printf sotto, sembra che il \n faccia baggare la count e non la stringa (tipo una buffer overflow nella struct dove sembra si inserisca il \n nell'elemento sotto della struct)
        }
    }

    fflush(stdout);

    free(gather_length);
    free(gather_buffer);
    free(local_count);
    MPI_Type_free(&mpi_char_int_pair);
    MPI_Finalize();
}
