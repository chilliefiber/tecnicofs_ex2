#include "operations.h"
#include "common/comms.h"
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <limits.h>
#include <string.h>
#include <errno.h>

#define S (1)
#define PC_BUF_SIZE (PIPE_BUF) // PIPE_BUF chosen because this way it can fit an entire message from the client

pthread_mutex_t mutex[S] = {PTHREAD_MUTEX_INITIALIZER};
pthread_mutex_t free_session_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t can_write[S] = {PTHREAD_COND_INITIALIZER};
pthread_cond_t can_read[S] = {PTHREAD_COND_INITIALIZER};
size_t prodptr[S] = {0};
size_t consptr[S] = {0};
size_t count[S] = {0};
char pc_buffer[S][PIPE_BUF]; 
char taken_session[S] = {0};

int free_session_id(int session_id) {
    if (pthread_mutex_lock(&
}

int get_session_id(bool *free_sessions) { // an int because session_id are ints
    for (int i = 0; i < S; i++) {
        if (free_sessions[i]) {
            free_sessions[i] = 0;
            return i;
        }
    }
    return -1;
}

void _read_from_buffer_unsychronized(void *dest, size_t len, char *pc_buffer, size_t *consptr, size_t *count) {
    *count -= len; // right now this is not valid, but will be at the end of the function
    // if we do not need to go around
    if ((*consptr) + len <= PC_BUF_SIZE) {
        memcpy(dest, pc_buffer + (*consptr), len);
        *consptr += len;
        if ((*consptr) == PC_BUF_SIZE)
            *consptr = 0;
        return;
    }
    size_t to_read_from_end = PC_BUF_SIZE - (*consptr); // bytes to read from the end of the buffer before we circle back to the beginning
    size_t to_read_from_start = len - to_read_from_end; // bytes to read from the start after we circle back
    memcpy(dest, pc_buffer + (*consptr), to_read_from_end); // after this call, *consptr is 0 but was not updated
    memcpy(dest + to_read_from_end, pc_buffer, to_read_from_start); 
    *consptr = to_read_from_start; // no need to check if it is PC_BUF_SIZE: since we read some from the end, at most it is the value of *consptr at the start of the function
}

int read_from_buffer(void *dest, size_t len, int session_id) {
    int ret_code = 0;
    /*
    if ((ret_code = pthread_mutex_lock(&mutex[session_id])) != 0)
        return ret_code;
    */
    // should I acquire the mutex here???
    while (count[session_id] < len) {
        if ((ret_code = pthread_cond_wait(&can_read[session_id], &mutex[session_id])) != 0) {
            return ret_code;
        }
    }
    _read_from_buffer_unsychronized(dest, len, pc_buffer[session_id], &consptr[session_id], &count[session_id]);
    pthread_cond_signal(&can_write[session_id]);
    /*ret_code = pthread_mutex_unlock(&mutex[session_id]);
    */
    return ret_code; 
}

void *worker(void *arg) {
    int *arg_int = (int *) arg;
    int session_id = *arg_int, send_id, recv_id;
    free(arg_int);
    char opcode;
    int ret_code, ret_error;
    char pipe_name[FIFO_NAME_SIZE+1];
    if (session_id < 0 || session_id >= S)
        return NULL;
    int write_fd = -1;
    // there is an agreement that the receiver task will 
    // only send full messages: here we just know that we 
    // never receive partial ones. This simplifies the worker task
    // but it has one tradeoff: if the buffer is not full but
    // the next entire message won't fit in it, we won't be able to send
    // any bytes: this is ok, because sending partial messages wouldn't
    // help anything
    while (1) {
        if (pthread_mutex_lock(&mutex[session_id]) != 0) {
            return NULL;
            // what to do here?            
        }
        while (count[session_id] == 0) {
            if (pthread_cond_wait(&can_read[session_id], &mutex[session_id]) != 0) {
                // what do?
                return NULL;
            }
        }
        opcode = pc_buffer[session_id][consptr];
        if (opcode == TFS_OP_CODE_MOUNT) {
            if ((ret_error = read_from_buffer(pipe_name, FIFO_NAME_SIZE, session_id)) != 0) {
                fprintf(stderr, "Error reading from buffer in session %d: %s\n", session_id, strerror(ret_error));
                return NULL;
            }
            pipe_name[FIFO_NAME_SIZE] = '\0'; 
            if ((write_fd = open(buffer, O_WRONLY)) == -1) {
                perror("Error opening client's fifo");
                return NULL;
            }

            send_id = session_id;
            if (tfs_init() == -1)
                send_id = -1;
            if (write_all(write_fd, &send_id, sizeof(int)) != 0) {
                perror("Error writing to client's fifo");
                close(write_fd); 
                return NULL;
            }
        }
        else if (opcode == TFS_OP_CODE_UNMOUNT) {
            if ((ret_error = read_from_buffer(&recv_id, sizeof(recv_id), session_id) != 0)) {
                fprintf(stderr, "Error reading from buffer in session %d: %s\n", session_id, strerror(ret_error));
                return NULL;
            }
            ret_code = 0; // we need to change this: right now unmount should always just return 0 as it does nothing apart
                          // from closing the write file descriptor: we need to check
            if (write_all(write_fd, &ret_code, sizeof(int)) != 0) {
                perror("Error writing to client's fifo in unmount");
                unlink(pipename);
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
            read_from_buffer(pc_buffer[session_id], &consptr[session_id], s, pipe_name, size_t *counti)      
            if (read_all(read_fd, buffer, sizeof(session_id) + MAX_FILE_NAME + sizeof(flags)) != 0) {
                perror("Error reading session id in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
            memcpy(&session_id, buffer, sizeof(session_id));
            offset = sizeof(session_id);
            memcpy(filename, buffer + offset, MAX_FILE_NAME);
            offset += MAX_FILE_NAME;
            memcpy(&flags, buffer + offset, sizeof(flags)); 
            ret_code = tfs_open(filename, flags);
            if (write_all(write_fd, &ret_code, sizeof(int)) != 0) {
                perror("Error writing to client's fifo in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
        }
        else if (opcode == TFS_OP_CODE_CLOSE) {
            if (read_all(read_fd, buffer, sizeof(session_id) + sizeof(fhandle)) != 0) {
                perror("Error reading session id in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
            memcpy(&session_id, buffer, sizeof(session_id));
            offset = sizeof(session_id);
            memcpy(&fhandle, buffer + offset, sizeof(fhandle)); 
            ret_code = tfs_close(fhandle);
            if (write_all(write_fd, &ret_code, sizeof(int)) != 0) {
                perror("Error writing to client's fifo in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
        }
        else if (opcode == TFS_OP_CODE_WRITE) {
            if (read_all(read_fd, buffer, sizeof(session_id) + sizeof(fhandle) + sizeof(len)) != 0) {
                perror("Error reading session id in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
            memcpy(&session_id, buffer, sizeof(session_id));
            offset = sizeof(session_id);
            memcpy(&fhandle, buffer + offset, sizeof(fhandle));
            offset += sizeof(fhandle);
            memcpy(&len, buffer + offset, sizeof(len));
            if (len > 0 && read_all(read_fd, buffer, len) != 0) {
                perror("Error reading session id in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
            // note that if len is 0 it doesn't matter what buffer has
            ret_code = (int) tfs_write(fhandle, buffer, len); // this is a weird cast: the communication protocol uses int but the fs uses ssize_t
            if (write_all(write_fd, &ret_code, sizeof(int)) != 0) {
                perror("Error writing to client's fifo in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
        }
        else if (opcode == TFS_OP_CODE_READ) {
            if (read_all(read_fd, buffer, sizeof(session_id) + sizeof(fhandle) + sizeof(len)) != 0) {
                perror("Error reading session id in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
            memcpy(&session_id, buffer, sizeof(session_id));
            offset = sizeof(session_id);
            memcpy(&fhandle, buffer + offset, sizeof(fhandle));
            offset += sizeof(fhandle);
            memcpy(&len, buffer + offset, sizeof(len));
            ret_code = (int) tfs_read(fhandle, buffer, len); // weird cast
            if (write_all(write_fd, &ret_code, sizeof(int)) != 0) {
                perror("Error writing to client's fifo in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
            if (ret_code > 0 && write_all(write_fd, buffer, (size_t) ret_code) != 0) {
                perror("Error writing to client's fifo in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
        }
        else if (opcode == TFS_OP_CODE_SHUTDOWN_AFTER_ALL_CLOSED) {
            if (read_all(read_fd, &session_id, sizeof(session_id)) != 0) {
                perror("Error reading session id in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
            ret_code = tfs_destroy_after_all_closed();
            if (write_all(write_fd, &ret_code, sizeof(ret_code)) != 0) {
                perror("Error writing to client's fifo in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
        }
        else {
            fprintf(stderr, "Received incorrect opcode\n");
            return -1; 
        }
         
    }
    return NULL; // will never get here
}

int main(int argc, char **argv) {

    if (argc < 2) {
        printf("Please specify the pathname of the server's pipe.\n");
        return 1;
    }

    char *pipename = argv[1];
    printf("Starting TecnicoFS server with pipe called %s\n", pipename);

    // create server's fifo
    if (unlink(pipename) != 0 && errno != ENOENT) {
        perror("Error unlinking");
        return -1;
    } 
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
     
    int write_fd = -1, session_id, flags, fhandle, ret_code;
    size_t offset, len; 
    char opcode;
    char buffer[PIPE_BUF]; // writes should be no bigger than PIPE_BUF, to make sure they are atomic in the FIFO. Actually it doesn't matter here in the server, but do it in the client
    char filename[MAX_FILE_NAME];
    bool free_session[S] = {true};
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

            if (tfs_init() == -1)
                session_id = -1;
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
        }
        else if (opcode == TFS_OP_CODE_UNMOUNT) {
            if (read_all(read_fd, &session_id, sizeof(session_id)) != 0) {
                perror("Error reading session id in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
            ret_code = 0; // we need to change this: right now unmount should always just return 0 as it does nothing apart
                          // from closing the write file descriptor: we need to check
            if (write_all(write_fd, &ret_code, sizeof(int)) != 0) {
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
            if (read_all(read_fd, buffer, sizeof(session_id) + MAX_FILE_NAME + sizeof(flags)) != 0) {
                perror("Error reading session id in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
            memcpy(&session_id, buffer, sizeof(session_id));
            offset = sizeof(session_id);
            memcpy(filename, buffer + offset, MAX_FILE_NAME);
            offset += MAX_FILE_NAME;
            memcpy(&flags, buffer + offset, sizeof(flags)); 
            ret_code = tfs_open(filename, flags);
            if (write_all(write_fd, &ret_code, sizeof(int)) != 0) {
                perror("Error writing to client's fifo in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
        }
        else if (opcode == TFS_OP_CODE_CLOSE) {
            if (read_all(read_fd, buffer, sizeof(session_id) + sizeof(fhandle)) != 0) {
                perror("Error reading session id in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
            memcpy(&session_id, buffer, sizeof(session_id));
            offset = sizeof(session_id);
            memcpy(&fhandle, buffer + offset, sizeof(fhandle)); 
            ret_code = tfs_close(fhandle);
            if (write_all(write_fd, &ret_code, sizeof(int)) != 0) {
                perror("Error writing to client's fifo in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
        }
        else if (opcode == TFS_OP_CODE_WRITE) {
            if (read_all(read_fd, buffer, sizeof(session_id) + sizeof(fhandle) + sizeof(len)) != 0) {
                perror("Error reading session id in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
            memcpy(&session_id, buffer, sizeof(session_id));
            offset = sizeof(session_id);
            memcpy(&fhandle, buffer + offset, sizeof(fhandle));
            offset += sizeof(fhandle);
            memcpy(&len, buffer + offset, sizeof(len));
            if (len > 0 && read_all(read_fd, buffer, len) != 0) {
                perror("Error reading session id in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
            // note that if len is 0 it doesn't matter what buffer has
            ret_code = (int) tfs_write(fhandle, buffer, len); // this is a weird cast: the communication protocol uses int but the fs uses ssize_t
            if (write_all(write_fd, &ret_code, sizeof(int)) != 0) {
                perror("Error writing to client's fifo in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
        }
        else if (opcode == TFS_OP_CODE_READ) {
            if (read_all(read_fd, buffer, sizeof(session_id) + sizeof(fhandle) + sizeof(len)) != 0) {
                perror("Error reading session id in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
            memcpy(&session_id, buffer, sizeof(session_id));
            offset = sizeof(session_id);
            memcpy(&fhandle, buffer + offset, sizeof(fhandle));
            offset += sizeof(fhandle);
            memcpy(&len, buffer + offset, sizeof(len));
            ret_code = (int) tfs_read(fhandle, buffer, len); // weird cast
            if (write_all(write_fd, &ret_code, sizeof(int)) != 0) {
                perror("Error writing to client's fifo in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
            if (ret_code > 0 && write_all(write_fd, buffer, (size_t) ret_code) != 0) {
                perror("Error writing to client's fifo in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
        }
        else if (opcode == TFS_OP_CODE_SHUTDOWN_AFTER_ALL_CLOSED) {
            if (read_all(read_fd, &session_id, sizeof(session_id)) != 0) {
                perror("Error reading session id in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
            ret_code = tfs_destroy_after_all_closed();
            if (write_all(write_fd, &ret_code, sizeof(ret_code)) != 0) {
                perror("Error writing to client's fifo in unmount");
                unlink(pipename);
                close(read_fd);
                close(write_fd); // should close all of them
                return -1;
            }
        }
        else {
            fprintf(stderr, "Received incorrect opcode\n");
            return -1; 
        }
    }
 
    return 0;
}
