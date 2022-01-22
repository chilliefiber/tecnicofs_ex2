#include "operations.h"
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <limits.h>

#define S (1)

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
     
    int write_fd, session_id;
    size_t offset; 
    char opcode;
    char buffer[PIPE_BUF]; // writes should be no bigger than PIPE_BUF, to make sure they are atomic in the FIFO. Actually it doesn't matter here in the server, but do it in the client
    char filename[MAX_FILE_NAME];
    while (1) {
        if (read(read_fd, &opcode, 1) != 1) {
            perror("Error reading opcode");
            unlink(pipename);
            close(read_fd);
            return -1;
        }
        if (opcode == TFS_OP_CODE_MOUNT) {
            if (read_all(read_fd, buffer, FIFO_NAME_SIZE) != 0) {
                perror("Error reading fifo's name in mount");
                return -1;
            }
            buffer[FIFO_NAME_SIZE] = '\0'; 
            session_id = 0;
            if ((write_fd = open(buffer, O_WRONLY)) == -1) {
                perror("Error opening client's fifo");
                unlink(pipename);
                close(read_fd);
                return -1;
            }

            if (write_all(write_fd, &session_id, sizeof(int)) != 0) {
                perror("Error writing to client's fifo");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
            
            /* This is extra code that will be useful when there are checks for too many sessions
            else if (close(write_fds[num_clients]) != 0) {
                perror("Error closing extra client's fifo");
                unlink(pipename);
                close(read_fd);
                close(write_fds[num_clients-1]); // should close all of them
                return -1;
            }*/
            tfs_init();
        }
        else if (opcode == TFS_OP_CODE_UNMOUNT) {
            if (read_all(read_fd, &session_id, sizeof(session_id)) != 0) {
                perror("Error reading session id in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
            session_id = 0; // we need to change this: right now unmount should always just return 0 as it does nothing apart
                            // from closing the write file descriptor: we need to check
            if (write_all(write_fd, &session_id, sizeof(int)) != 0) {
                perror("Error writing to client's fifo in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
            if (close(write_fd) != 0) {
                perror("Error closing write file descriptor in unmount");
                unlink(pipename);
                close(read_fd);
                return -1;
            }
        }
        else if (opcode == TFS_OP_CODE_OPEN) {
            if (read_all(read_fd, buffer, sizeof(session_id) + MAX_FILE_NAME + sizeof(int)) != 0) {
                perror("Error reading session id in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
            int flags;
            memcpy(
        }
    }
 
    return 0;
}
