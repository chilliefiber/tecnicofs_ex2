#include "operations.h"
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#define S (1)
#define BUFFER_SIZE (1000)

typedef struct session {
    int session_id;
    int write_fd; // possibly change to char[FIFO_NAME_SIZE]
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
   
    int read_fd = open(pipename, O_RDONLY);

    if (read_fd == -1) {
        perror("Error opening server's FIFO for reading");
        unlink(pipename);
        return -1;
    }
   
    // one extra for someone that attempts a connection when it is already full 
    int write_fds[S+1], num_clients = 0, session_id;

    char opcode;
    char buffer[BUFFER_SIZE];

    while (1) {
        if (read(read_fd, &opcode, 1) != 1) {
            perror("Error reading opcode");
            unlink(pipename);
            close(read_fd);
            return -1;
        }
        if (opcode == TFS_OP_CODE_MOUNT) {
            if (read(read_fd, buffer, FIFO_NAME_SIZE) != FIFO_NAME_SIZE)
                continue;
            buffer[FIFO_NAME_SIZE + 1] = '\0';
            if (num_clients < S) 
                session_id = num_clients;
            else
                session_id = -1;
            if ((write_fds[num_clients] = open(buffer, O_WRONLY)) == -1) {
                perror("Error opening client's fifo");
                unlink(pipename);
                close(read_fd);
                return -1;
            }

            if (write(write_fds[num_clients], &session_id, sizeof(int)) != sizeof(int)) {
                perror("Error writing to client's fifo");
                unlink(pipename);
                close(read_fd);
                close(write_fds[num_clients]); // should close all of them
                return -1;
            }

            if (session_id != -1)
                num_clients++;
            else if (close(write_fds[num_clients]) != 0) {
                perror("Error closing extra client's fifo");
                unlink(pipename);
                close(read_fd);
                close(write_fds[num_clients-1]); // should close all of them
                return -1;
            }
        }
    }
 
    return 0;
}
