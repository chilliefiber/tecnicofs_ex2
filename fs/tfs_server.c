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
#include <stdbool.h>
#include <pthread.h>

#define S (1)
#define PC_BUF_SIZE (PIPE_BUF) // PIPE_BUF chosen because this way it can fit an entire message from the client

pthread_mutex_t mutex[S] = {PTHREAD_MUTEX_INITIALIZER};
pthread_mutex_t free_session_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t can_write[S] = {PTHREAD_COND_INITIALIZER};
pthread_cond_t can_read[S] = {PTHREAD_COND_INITIALIZER};
size_t prodptrs[S] = {0};
size_t consptrs[S] = {0};
size_t counts[S] = {0};
char pc_buffers[S][PIPE_BUF]; 
char taken_session[S] = {0};

inline bool valid_session_id(int session_id) {
    return (session_id > -1 && session_id < S);
}

int free_session_id(int session_id) {
    if (!valid_session_id(session_id))
        return -1;
    int ret_code = 0;
    if (pthread_mutex_lock(&free_session_mutex) != 0)
        return -1;
    if (!taken_session[session_id])
        ret_code = -1;
    taken_session[session_id] = 1;
    if (pthread_mutex_unlock(&free_session_mutex) != 0)
        return -1;
    return ret_code;
}

int get_session_id() { // an int because session_id are ints
    if (pthread_mutex_lock(&free_session_mutex) != 0)
        return -1;
    for (int i = 0; i < S; i++) {
        if (!taken_session[i]) {
            taken_session[i] = 1;
            if (pthread_mutex_unlock(&free_session_mutex) != 0) {
                return -1; 
            }
            return i;
        }
    }
    pthread_mutex_unlock(&free_session_mutex);
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
    while (counts[session_id] < len) {
        if ((ret_code = pthread_cond_wait(&can_read[session_id], &mutex[session_id])) != 0) {
            return ret_code;
        }
    }
    _read_from_buffer_unsychronized(dest, len, pc_buffers[session_id], &consptrs[session_id], &counts[session_id]);
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
    int flags, fhandle;
    char pipe_name[FIFO_NAME_SIZE+1], file_name[MAX_FILE_NAME], rw_buffer[PIPE_BUF];
    if (session_id < 0 || session_id >= S)
        return NULL;
    int write_fd = -1;
    size_t len;
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
        if ((ret_error = read_from_buffer(&opcode, 1, session_id) != 0)) {
                fprintf(stderr, "Error reading opcode from buffer in session %d: %s\n", session_id, strerror(ret_error));
                return NULL;
        }  
        if (opcode == TFS_OP_CODE_MOUNT) {
            if ((ret_error = read_from_buffer(pipe_name, FIFO_NAME_SIZE, session_id)) != 0) {
                fprintf(stderr, "Error reading pipe name from buffer in mount session %d: %s\n", session_id, strerror(ret_error));
                return NULL;
            }
            pipe_name[FIFO_NAME_SIZE] = '\0'; 
            if ((write_fd = open(pipe_name, O_WRONLY)) == -1) {
                perror("Error opening client's fifo in mount");
                return NULL;
            }

            send_id = session_id;
            if (tfs_init() == -1)
                send_id = -1;
            if (write_all(write_fd, &send_id, sizeof(int)) != 0) {
                perror("Error writing to client's fifo in mount");
                close(write_fd); 
                // write_fd = -1; in case we don't return
                return NULL;
            }
            if (send_id == -1) {
                close(write_fd);
                write_fd = -1;
            }
        }
        else if (opcode == TFS_OP_CODE_UNMOUNT) {
            if ((ret_error = read_from_buffer(&recv_id, sizeof(recv_id), session_id) != 0)) {
                fprintf(stderr, "Error reading session_id from buffer in unmount session %d: %s\n", session_id, strerror(ret_error));
                return NULL;
            }
            ret_code = free_session_id(session_id); 
            if (write_all(write_fd, &ret_code, sizeof(int)) != 0) {
                perror("Error writing to client's fifo in unmount");
                close(write_fd);
                return NULL;
            }
            if (close(write_fd) != 0) {
                perror("Error closing write file descriptor in unmount");
                return NULL;
            }
        }
        else if (opcode == TFS_OP_CODE_OPEN) {
            if ((ret_error = read_from_buffer(&recv_id, sizeof(recv_id), session_id)) != 0) {
                fprintf(stderr, "Error reading session_id from buffer in open session %d: %s\n", session_id, strerror(ret_error));
                return NULL;
            }
            if ((ret_error = read_from_buffer(&file_name, sizeof(file_name), session_id)) != 0) {
                fprintf(stderr, "Error reading file name from buffer in open session %d: %s\n", session_id, strerror(ret_error));
                return NULL;
            }
            file_name[MAX_FILE_NAME - 1] = '\0'; // we need to make sure the last character is a \0: inside the filesystem strlen is called with the filename
            if ((ret_error = read_from_buffer(&flags, sizeof(flags), session_id)) != 0) {
                fprintf(stderr, "Error reading flags from buffer in open session %d: %s\n", session_id, strerror(ret_error));
                return NULL;
            }
            ret_code = tfs_open(file_name, flags);
            if (write_all(write_fd, &ret_code, sizeof(int)) != 0) {
                perror("Error writing to client's fifo in open");
                close(write_fd);
                return NULL;
            }
        }
        else if (opcode == TFS_OP_CODE_CLOSE) {
            if ((ret_error = read_from_buffer(&recv_id, sizeof(recv_id), session_id)) != 0) {
                fprintf(stderr, "Error reading session_id from buffer in close session %d: %s\n", session_id, strerror(ret_error));
                return NULL;
            }
            if ((ret_error = read_from_buffer(&fhandle, sizeof(fhandle), session_id)) != 0) {
                fprintf(stderr, "Error reading fhandle from buffer in close session %d: %s\n", session_id, strerror(ret_error));
                return NULL;
            }
            ret_code = tfs_close(fhandle);
            if (write_all(write_fd, &ret_code, sizeof(int)) != 0) {
                perror("Error writing to client's fifo in unmount");
                close(write_fd); // should close all of them
                return NULL;
            }
        }
        else if (opcode == TFS_OP_CODE_WRITE) {
            if ((ret_error = read_from_buffer(&recv_id, sizeof(recv_id), session_id)) != 0) {
                fprintf(stderr, "Error reading session_id from buffer in write session %d: %s\n", session_id, strerror(ret_error));
                return NULL;
            }
            if ((ret_error = read_from_buffer(&fhandle, sizeof(fhandle), session_id)) != 0) {
                fprintf(stderr, "Error reading fhandle from buffer in write session %d: %s\n", session_id, strerror(ret_error));
                return NULL;
            }
            if ((ret_error = read_from_buffer(&len, sizeof(len), session_id)) != 0) {
                fprintf(stderr, "Error reading len from buffer in write session %d: %s\n", session_id, strerror(ret_error));
                return NULL;
            }
            if (len > 0 && (ret_error = read_from_buffer(rw_buffer, len, session_id)) != 0) {
                fprintf(stderr, "Error reading bytes to write from buffer in write session %d: %s\n", session_id, strerror(ret_error));
                close(write_fd); // should close all of them
                // write_fd = -1;
                return NULL;
            }
            // note that if len is 0 it doesn't matter what rw_buffer has
            ret_code = (int) tfs_write(fhandle, rw_buffer, len); // this is a weird cast: the communication protocol uses int but the fs uses ssize_t
            if (write_all(write_fd, &ret_code, sizeof(int)) != 0) {
                perror("Error writing to client's fifo in unmount");
                close(write_fd); // should close all of them
                // write_fd = -1;
                return NULL;
            }
        }
        else if (opcode == TFS_OP_CODE_READ) {
            if ((ret_error = read_from_buffer(&recv_id, sizeof(recv_id), session_id)) != 0) {
                fprintf(stderr, "Error reading session_id from buffer in read session %d: %s\n", session_id, strerror(ret_error));
                close(write_fd);
                return NULL;
            }
            if ((ret_error = read_from_buffer(&fhandle, sizeof(fhandle), session_id)) != 0) {
                fprintf(stderr, "Error reading fhandle from buffer in read session %d: %s\n", session_id, strerror(ret_error));
                close(write_fd);
                return NULL;
            }
            if ((ret_error = read_from_buffer(&len, sizeof(len), session_id)) != 0) {
                fprintf(stderr, "Error reading len from buffer in read session %d: %s\n", session_id, strerror(ret_error));
                close(write_fd);
                return NULL;
            }
            ret_code = (int) tfs_read(fhandle, rw_buffer, len); // weird cast
            if (write_all(write_fd, &ret_code, sizeof(int)) != 0) {
                perror("Error writing return code to client's fifo in read");
                close(write_fd); // should close all of them
                return NULL;
            }
            if (ret_code > 0 && write_all(write_fd, rw_buffer, (size_t) ret_code) != 0) {
                perror("Error writing bytes to client's fifo in read");
                close(write_fd); // should close all of them
                return NULL;
            }
        }
        else if (opcode == TFS_OP_CODE_SHUTDOWN_AFTER_ALL_CLOSED) {
            if ((ret_error = read_from_buffer(&recv_id, sizeof(recv_id), session_id)) != 0) {
                fprintf(stderr, "Error reading session_id from buffer in shutdown session %d: %s\n", session_id, strerror(ret_error));
                close(write_fd);
                return NULL;
            }
            ret_code = tfs_destroy_after_all_closed();
            if (free_session_id(session_id) != 0)
                ret_code = -1;
            if (write_all(write_fd, &ret_code, sizeof(ret_code)) != 0) {
                perror("Error writing to client's fifo in shutdown");
                close(write_fd); // should close all of them
                return NULL;
            }
        }
        else {
            fprintf(stderr, "Received incorrect opcode\n");
            return NULL; 
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
