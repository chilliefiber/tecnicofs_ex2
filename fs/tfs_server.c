#include "operations.h"
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#define S (1)

typedef struct session {
    size_t session_id;
    char *client_fifo; // possibly change to char[FIFO_NAME_SIZE]
} session;

int main(int argc, char **argv) {

    if (argc < 2) {
        printf("Please specify the pathname of the server's pipe.\n");
        return 1;
    }

    char *pipename = argv[1];
    printf("Starting TecnicoFS server with pipe called %s\n", pipename);

    // create server's fifo
    
    if (mkfifo(pipename, 0777) != 0) {
        perror("Error creating named pipe");
        return -1;
    }
   
    int server_fifo_fd = open(pipename, O_RDONLY);

    if (server_fifo_fd == -1) {
        perror("Error opening server's FIFO for reading");
        unlink(pipename);
        return -1;
    }
    
    int num_clients = 0;
 
    return 0;
}
