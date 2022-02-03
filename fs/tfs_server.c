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

#define S (20)
#define PC_BUF_SIZE (PIPE_BUF) // PIPE_BUF chosen because this way it can fit an entire message from the client

#define OP_CODE_FREE_SESSION 8 // we do all the work related to the thread in the thread: if there's a need to free a session, it is done by the thread itself
                               // this simplifies a problem: in freeing a session, we need to close the write fd related to it: by delegating this task to the 
                               // thread, there's no need to protect that fd with a mutex 
typedef struct command {
    char opcode;
    int session_id;
    int fhandle;
    int flags;
    size_t len;
    char file_name[MAX_FILE_NAME + 1]; 
    char fifo_name[FIFO_NAME_SIZE + 1];
    char rw_buffer[PIPE_BUF]; // this buffer could be dinamically allocated as well, which would make more sense in an environment with more stern memory
                           // constraints. I chose not to do that because it was extra trouble for little reward. I could have also only used one buffer
                           // but I kept it like this to logically separate the buffer for the filename and the fifo name and the buffer for the reads/writes: again,
                           // we have plenty of memory
} command;

pthread_mutex_t exit_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t session_mutex[S] = {PTHREAD_MUTEX_INITIALIZER}; // this mutex will cover the prodptr, consptr, count and write_fd of the session
                                                                // using a separate mutex for the write fd would be too much memory for little reward
pthread_mutex_t free_session_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t can_write[S] = {PTHREAD_COND_INITIALIZER};
pthread_cond_t can_read[S] = {PTHREAD_COND_INITIALIZER};
size_t prodptrs[S] = {0};
size_t consptrs[S] = {0};
size_t counts[S] = {0};
char pc_buffers[S][PIPE_BUF]; 
char taken_session[S] = {0};
int write_fds[S] = {-1};


int main(int argc, char **argv) {
    if (argc < 2) {
        printf("Please specify the pathname of the server's pipe.\n");
        return 1;
    }

    if (signal(SIGPIPE, SIG_IGN) == SIG_ERR) {
        perror("Error ignoring sigpipe");
        return -1;
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
   
    int aux_read_fd, read_fd;
    while (read_fd = open(pipename, O_RDONLY) == -1) {
        if (errno == EINTR)
            continue;
        perror("Error opening server's FIFO for reading");
        unlink(pipename);
        return -1;
    }
 
    if (tfs_init() == -1) {
        fprintf(stderr, "We couldn't initialize the file system\n");
        return -1;
    }

    ssize_t read_ret_code; 
    int write_fd = -1, session_id, flags, fhandle, ret_code, *worker_id;
    size_t len; 
    char opcode;
    char buffer[PIPE_BUF]; // writes should be no bigger than PIPE_BUF, to make sure they are atomic in the FIFO. Actually it doesn't matter here in the server, but do it in the client
    pthread_t workers[S];
    for (int i = 0; i < S; i++) {
        worker_id = malloc(sizeof(int));
        if (worker_id == NULL) {
            fprintf(stderr, "Error allocing argument for thread %d\n", i);
            return -1;
        }
        *worker_id = i;
        if ((ret_code = pthread_create(&workers[i], NULL, worker, (void *) worker_id)) != 0) {
            fprintf(stderr, "Error creating thread: %s\n", strerror(ret_code));
            return -1;
        }
        if ((ret_code = pthread_detach(workers[i])) != 0) {
            fprintf(stderr, "Error detaching thread: %s\n", strerror(ret_code));
            return -1;
        }
    }
    
    while (1) {
        if ((read_ret_code = read(read_fd, &opcode, 1)) < 0 && errno != EINTR) {
            perror("Main thread: Error reading opcode");
            unlink(pipename);
            close(read_fd);
            return -1;
        }
        else if (errno == EINTR)
            continue;
        else if (read_ret_code == 0) {// this happens when there are no clients that have opened the file for writing (they all closed it already)
            while (aux_read_fd = open(pipename, O_RDONLY) == -1) {
                if (errno == EINTR)
                    continue;
                perror("Error opening server's FIFO for reading"); // we close the server here: not being able to open the FIFO is a catastrophic error
                close(read_fd);
                unlink(pipename);
                return -1;
            } // wait here for someone to open
            if (close(aux_read_fd) != 0) {
                perror("Error closing auxiliary read file descriptor"); // here I decided not to close the server: the other fd is still open and might be usable
            }
            continue;
        }
        parse_command(read_fd, opcode);
    }
 
    return 0;
}

parse_command(int read_fd, char opcode) {
    command *cmd = malloc(sizeof(command)); // I allocate it in this function for code reuse: every function would have to call malloc 
                                            // and check for errors, and this way it's all done in one place. The only tradeoff is
                                            // I still malloc for invalid opcodes
    int ret_code;
    if (cmd == NULL) {
        fprintf(stderr, "Error mallocing command\n");        
        if (ret_code = pthread_mutex_lock(&exit_mutex) != 0) {
            fprintf(stderr, "Error locking exit_mutex in parse_command: %s\n", strerror(ret_code));
        }
        exit(EXIT_FAILURE); // I exit anyway to make sure the program crashes: not sure if I should, since exit is not MT safe. However, in my man page (Linux, not POSIX)
        // none of the errors are likely to happen: EINVAL because the mutex is always properly initialized (with the macro), and EDEADLOCK because this is not recursive
    }
    cmd->opcode = opcode;
    switch (opcode) {
        case TFS_OP_CODE_MOUNT:
            parse_mount(read_fd, cmd);
            return;
        case TFS_OP_CODE_UNMOUNT:
            parse_unmount(read_fd, cmd);
            return;
        case TFS_OP_CODE_OPEN:
            parse_open(read_fd, cmd);
            return;
        case TFS_OP_CODE_CLOSE:
            parse_close(read_fd, cmd);
            return;
        case TFS_OP_CODE_WRITE:
            parse_write(read_fd, cmd);
            return;
        case TFS_OP_CODE_READ:
            parse_read(read_fd, cmd);
            return;
        case TFS_OP_CODE_SHUTDOWN_AFTER_ALL_CLOSED:
            parse_shutdown(read_fd, cmd);
            return;
        default:
            free(cmd);
            fprintf(stderr, "Received invalid opcode with value: %d\n", opcode);
            return;
    }
}

void parse_mount(int read_fd, command *cmd) {
    int write_fd;
    int session_id;
    if (read_all(read_fd, cmd->fifo_name, FIFO_NAME_SIZE) != 0) {
        free(cmd);
        perror("Error reading fifo's name in mount"); // we continue execution: this might be problematic for some errors in read... I figure they will then appear 
                                                      // when we read the next opcode
        return;
    }
    cmd->fifo_name[FIFO_NAME_SIZE] = '\0'; 
    session_id = get_session_id();
    // I always call perror: note that there is a chance for example in write_all
    // that errno was not updated. This is true in lots of places around the code, but I figure that's ok
    if (session_id == -1) {
        fprintf(stderr, "Main thread will reject a client\n");
        if ((write_fd = open(cmd->fifo_name, O_WRONLY)) == -1) {
            free(cmd);
            perror("Error opening client's fifo in mount");
            return; 
        }
        free(cmd);
        if (write_all(write_fd, &session_id, sizeof(int)) != 0) {
            perror("Error writing to client's fifo");
        }
        if (close(write_fd) != 0)
            perror("Error closing fd for rejected client in mount");
        fprintf(stderr, "main thread rejected a client\n");
    }
    else {
        cmd->session_id = session_id;
        if ((ret_code = write_into_buffer(cmd, sizeof(cmd), session_id)) != 0) {
            fprintf(stderr, "Error writing to producer consumer buffer in session %d mount: %s\n", session_id);
            free(cmd);
            terminate_session(session_id);
        }
    }
}

void parse_unmount(int read_fd, command *cmd) {
    if (read_all(read_fd, &(cmd->session_id), sizeof(session_id)) != 0) {
        perror("Error reading session id in unmount");
        free(cmd);
        return;
    }
    if (!active_session_id(cmd->session_id)) {
        fprintf(stderr, "Received unmount command for inactive session %d\n", cmd->session_id);
        free(cmd);
        return;
    }
    if ((write_into_buffer(cmd, sizeof(cmd), cmd->session_id)) != 0) {
        fprintf(stderr, "Error writing to producer consumer buffer in session %d unmount\n", cmd->session_id);
        terminate_session(cmd->session_id);
        free(cmd);
    }
}

void parse_open(int read_fd, command *cmd) {
    if (read_all(read_fd, &cmd->session_id, sizeof(int) ) != 0) {
        perror("Error reading session id in open");
        free(cmd);
        return;
    }
    if (!active_session_id(cmd->session_id)) {
        fprintf(stderr, "Received open command for inactive session %d\n", cmd->session_id);
        free(cmd);
        return;
    }
    if (read_all(read_fd, cmd->filename, MAX_FILE_NAME ) != 0) {
        fprintf(stderr, "Error reading filename in open session %d: %s\n"; cmd->session_id, strerror(errno)); 
        terminate_session(cmd->session_id);
        free(cmd);
        return;
    }
    cmd->filename[MAX_FILE_NAME] = '\0';
    if (read_all(read_fd, &cmd->flags, sizeof(int)) != 0) {
        fprintf(stderr, "Error reading flags in open session %d: %s\n"; cmd->session_id, strerror(errno)); 
        terminate_session(cmd->session_id);
        free(cmd);
        return;
    }
    if ((ret_code = write_into_buffer(cmd, sizeof(cmd), cmd->session_id)) != 0) {
        fprintf(stderr, "Error writing to producer consumer buffer in session %d open\n", cmd->session_id);
        terminate_session(cmd->session_id);
        free(cmd);
    }
}

void parse_close(int read_fd, command *cmd) {
    if (read_all(read_fd, &cmd->session_id, sizeof(session_id) + ) != 0) {
        perror("Error reading session id in close");
        free(cmd);
        return;
    }
    if (!active_session_id(cmd->session_id)) {
        fprintf(stderr, "Received close command for inactive session %d\n", cmd->session_id);
        free(cmd);
        return;
    }
    if (read_all(read_fd, &cmd->fhandle, sizeof(int)) != 0) {
        fprintf(stderr, "Error reading fhandle in open session %d: %s\n"; cmd->session_id, strerror(errno)); 
        terminate_session(cmd->session_id);
        free(cmd);
        return;
    }
    if ((ret_code = write_into_buffer(cmd, sizeof(cmd), cmd->session_id)) != 0) {
        fprintf(stderr, "Error writing to producer consumer buffer in session %d close\n", cmd->session_id);
        terminate_session(cmd->session_id);
        free(cmd);
    }
}

void parse_write(int read_fd, command *cmd) {
    if (read_all(read_fd, &cmd->session_id, sizeof(session_id) ) != 0) {
        perror("Error reading session id in write");
        free(cmd);
        return;
    }
    if (!active_session_id(cmd->session_id)) {
        fprintf(stderr, "Received write command for inactive session %d\n", cmd->session_id);
        free(cmd);
        return;
    }
    if (read_all(read_fd, &cmd->fhandle, sizeof(int)) != 0) {
        fprintf(stderr, "Error reading fhandle in write session %d: %s\n"; cmd->session_id, strerror(errno)); 
        terminate_session(cmd->session_id);
        free(cmd);
        return;
    }
    if (read_all(read_fd, &cmd->len, sizeof(size_t)) != 0) {
        fprintf(stderr, "Error reading len in write session %d: %s\n"; cmd->session_id, strerror(errno)); 
        terminate_session(cmd->session_id);
        free(cmd);
        return;
    }
    if (cmd->len > 0 && read_all(read_fd, cmd->rw_buffer, len) != 0) {
        fprintf(stderr, "Error reading bytes to write in write session %d: %s\n", cmd->session_id, strerror(errno));
        terminate_session(cmd->session_id);
        free(cmd);
        return;
    }
    if ((ret_code = write_into_buffer(cmd, sizeof(cmd), cmd->session_id)) != 0) {
        fprintf(stderr, "Error writing to producer consumer buffer in session %d write\n", cmd->session_id);
        terminate_session(cmd->session_id);
        free(cmd);
    }
}

void parse_read(int read_fd, command *cmd) {
    if (read_all(read_fd, &cmd->session_id, sizeof(session_id) ) != 0) {
        perror("Error reading session id in read");
        free(cmd);
        return;
    }
    if (!active_session_id(cmd->session_id)) {
        fprintf(stderr, "Received read command for inactive session %d\n", cmd->session_id);
        free(cmd);
        return;
    }
    if (read_all(read_fd, &cmd->fhandle, sizeof(int)) != 0) {
        fprintf(stderr, "Error reading fhandle in read session %d: %s\n"; cmd->session_id, strerror(errno)); 
        terminate_session(cmd->session_id);
        free(cmd);
        return;
    }
    if (read_all(read_fd, &cmd->len, sizeof(size_t)) != 0) {
        fprintf(stderr, "Error reading len in read session %d: %s\n"; cmd->session_id, strerror(errno)); 
        terminate_session(cmd->session_id);
        free(cmd);
        return;
    }
    if ((ret_code = write_into_buffer(cmd, sizeof(cmd), cmd->session_id)) != 0) {
        fprintf(stderr, "Error writing to producer consumer buffer in session %d read\n", cmd->session_id);
        terminate_session(cmd->session_id);
        free(cmd);
    }
}

void parse_shutdown(int read_fd, command *cmd) {
    if (read_all(read_fd, cmd->session_id, sizeof(int)) != 0) {
        perror("Error reading session id in shutdown after all closed");
        free(cmd);
        return;
    }
    if (!active_session_id(cmd->session_id)) {
        fprintf(stderr, "Received shutdown command for inactive session %d\n", cmd->session_id);
        free(cmd);
        return;
    }
    if ((ret_code = write_into_buffer(cmd, sizeof(cmd), cmd->session_id)) != 0) {
        fprintf(stderr, "Error writing to producer consumer buffer in session %d unmount: %s\n", cmd->session_id);
        terminate_session(cmd->session_id);
        free(cmd);
    }
}

bool valid_session_id(int session_id) {
    return (session_id > -1 && session_id < S);
}

bool active_session(int session_id) {
    bool active = false;
    int ret_code;
    if (valid_session_id(session_id)) {
        if ((ret_code = pthread_mutex_lock(&free_session_mutex)) != 0) {
            fprintf(stderr, "Error locking free_session_mutex in active_session_id(): %s\n", strerror(ret_code));
            return active;
        }
        active = taken_session[session_id];
        if ((ret_code = pthread_mutex_unlock(&free_session_mutex)) != 0) {
            fprintf(stderr, "Error unlocking free_session_mutex in active_session_id(): %s\n", strerror(ret_code));
            return false;
        }
    }
    else
        fprintf(stderr, "Invalid session_id sent to active_session_id: %d\n", session_id);
    return active;
}

// this function can be called by the receiver thread and the worker thread, but not at the same time
// because the client is sequential so the receiver thread will never receive a new command for the same session while
// the worker thread is working
void terminate_session(int session_id) {
   clear_session(session_id);
   free_session_id(session_id);  
}

void clear_session(int session_id) {
    int ret_code;
    if (!valid_session_id(session_id)) {
        fprintf(stderr, "Invalid session_id sent to clear_session: %d\n", session_id);
        return;
    }
    if ((ret_code = pthread_mutex_lock(&mutex[session_id])) != 0) {
        fprintf(stderr, "Error locking session %d mutex in free_session_id: %s\n", session_id, strerror(ret_code));
        return -1;
    }
    consptrs[session_id] = 0;
    prodptrs[session_id] = 0;
    counts[session_id] = 0;
    ret_code = -1;
    // when we are terminating a session, if there is an active file descriptor to communicate with
    // we send a -1
    if (write_fds[session_id] != -1 && write_all(write_fds[session_id], &ret_code, sizeof(int)) != 0) {
        fprintf(stderr, "Error writing to client's fifo when clearing session %d: %s\n", session_id, strerror(errno));
    }
    if (write_fds[session_id] != -1 && close(write_fds[session_id]) != 0)
        fprintf(stderr, "Error closing file descriptor %d for session %d: %s\n", write_fds[session_id], session_id, strerror(errno));
    write_fds[session_id] = -1;
    if ((ret_code = pthread_mutex_unlock(&mutex[session_id])) != 0) {
        fprintf(stderr, "Error unlocking session %d mutex in free_session_id: %s\n", session_id, strerror(ret_code));
        return;
    }
}

void free_session_id(int session_id) {
    if ((ret_code = pthread_mutex_lock(&free_session_mutex)) != 0) {
        fprintf(stderr, "Error locking free session mutex in free_session_id: %s\n", strerror(ret_code));
        return;
    }
    taken_session[session_id] = 0; 
    if ((ret_code = pthread_mutex_unlock(&free_session_mutex)) != 0) {
        fprintf(stderr, "Error unlocking free session mutex in free_session_id: %s\n", strerror(ret_code));
    } 
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

void _write_into_buffer_unsychronized(const void *src, size_t len, char *pc_buffer, size_t *prodptr, size_t *count) {
   *count += len;
    if ((*prodptr) + len <= PC_BUF_SIZE) {
        memcpy(pc_buffer + (*prodptr), src, len);
        *prodptr += len;
        if ((*prodptr) == PC_BUF_SIZE)
            *prodptr = 0;
        return; 
    }
    size_t to_write_in_end = PC_BUF_SIZE - (*prodptr);
    size_t to_write_in_start = len - to_write_in_end;
    memcpy(pc_buffer + (*prodptr), src, to_write_in_end);
    memcpy(pc_buffer, src + to_write_in_end, to_write_in_start);
    *prodptr = to_write_in_start; 
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
    if (!valid_session_id(session_id)) {
        fprintf(stderr, "Tried to read from a buffer without a valid session_id : %d\n", session_id);
        return TERMINATE_SESSION;
    }
    int ret_code = 0;
    size_t to_read, total_read = 0; 
    if ((ret_code = pthread_mutex_lock(&mutex[session_id])) != 0) {
        fprintf(stderr, "Error locking mutex in read_from_buffer, session %d: %s\n", session_id, strerror(ret_code));
        return TERMINATE_SESSION;
    }
    // this is a bit nasty but it allows for the (very unlikely if not impossible) chance that there are some bytes to be
    // read but not enough to fill dest. 
    while (len > 0) { 
        while (counts[session_id] == 0) { // note that if len > PC_BUF_SIZE this is a deadlock
            if ((ret_code = pthread_cond_wait(&can_read[session_id], &mutex[session_id])) != 0) {
                fprintf(stderr, "Error waiting in read_from_buffer, session %d: %s\n", session_id, strerror(ret_code));
                // according to linux manpage, pthread_cond_wait never returns an error. According to POSIX manpage
                // "all these error checks shall act as if they were performed 
                //immediately at the beginning of processing for the function and shall cause an error return, in effect, 
                // prior to modifying the state of the mutex specified by mutex or the condition variable specified by cond."
                // thus, the locked mutex would not have been unlocked, so we unlock it here
                if (ret_code = pthread_mutex_unlock(&mutex[session_id]) != 0)
                    fprintf(stderr, "Error unlocking mutex in read_from_buffer, session %d: %s\n", session_id, strerror(ret_code));
                return TERMINATE_SESSION;
            }
        }
        // we read only the necessary bytes, or the ones available if there are less than the necessary
        to_read = counts[session_id] < len ? counts[session_id] : len; 
        len -= to_read;
        _read_from_buffer_unsychronized(dest + total_read, to_read, pc_buffers[session_id], &consptrs[session_id], &counts[session_id]);
        total_read += to_read;
        if ((ret_code = pthread_cond_signal(&can_write[session_id])) != 0) {
            fprintf(stderr, "Error signalling ability to write to the producer consumer buffer of session %d: %s\n", session_id, strerror(ret_code));
            if (ret_code = pthread_mutex_unlock(&mutex[session_id]) != 0)
                fprintf(stderr, "Error unlocking mutex in read_from_buffer, session %d: %s\n", session_id, strerror(ret_code));
            return TERMINATE_SESSION;
        }
        
    }
    if (ret_code = pthread_mutex_unlock(&mutex[session_id]) != 0) {
        fprintf(stderr, "Error unlocking mutex in read_from_buffer, session %d: %s\n", session_id, strerror(ret_code));
        return TERMINATE_SESSION; 
    }
    return 0; 
}

int write_into_buffer(const void *src, size_t len, int session_id) {
    if (!valid_session_id(session_id)) {
        fprintf(stderr, "Tried to write into a pc buffer of an invalid session: %d\n", session_id);
        return TERMINATE_SESSION;
    }
    int ret_code = 0;
    if ((ret_code = pthread_mutex_lock(&mutex[session_id])) != 0) {
        fprintf(stderr, "Error locking session %d mutex in write_into_buffer: %s\n", session_id, strerror(ret_code));
        return TERMINATE_SESSION;
    }
    size_t to_write, total_written = 0, available_bytes;
    while (len > 0) {
        while (counts[session_id] == PC_BUF_SIZE) { 
            if ((ret_code = pthread_cond_wait(&can_write[session_id], &mutex[session_id])) != 0) {
                fprintf(stderr, "Error waiting to write in pc buffer session %d: %s\n", session_id, strerror(ret_code));
                return TERMINATE_SESSION;
            }
            // notice that if the client was not sequential, there would be a chance that the session
            // would no longer be active here. However with the sequential client what will happen will
            // be 1-receive command through pipe, 2-send command to pc buffer, 3-read command to pc buffer
            // 4- handle command in worker thread and send response and back to 1. For non sequential clients
            // this code is wrong, and so is read_into_buffer, because while someone was waiting another one
            // could terminate the session
        }
        available_bytes = PC_BUF_SIZE - counts[session_id];
        to_write = len > available_bytes ? available_bytes : len;
        _write_into_buffer_unsychronized(src + total_written, to_write, pc_buffers[session_id], &prodptrs[session_id], &counts[session_id]);
        total_written += to_write;
        len -= to_write;
        if ((ret_code = pthread_cond_signal(&can_read[session_id])) != 0) {
            fprintf(stderr, "Error signalling ability to read in session %d: %s\n", session_id, strerror(ret_code));
            if ((ret_code = pthread_mutex_unlock(&mutex[session_id])) != 0)
                fprintf(stderr, "Error unlocking mutex in write_into_buffer session %d: %s\n", session_id, strerror(ret_code));
            return TERMINATE_SESSION;
        }
    }
    if ((ret_code = pthread_mutex_unlock(&mutex[session_id])) != 0) {
        fprintf(stderr, "Error unlocking mutex in write_into_buffer session %d: %s\n", session_id, strerror(ret_code));
        return TERMINATE_SESSION;
    }
    return 0; 
}

execute_command(command *cmd) {
    switch (cmd->opcode) {
        case TFS_OP_CODE_MOUNT:
            execute_mount(cmd);
            return;
        case TFS_OP_CODE_UNMOUNT:
            execute_unmount(cmd);
            return;
        case TFS_OP_CODE_OPEN:
            execute_open(cmd);
            return;
        case TFS_OP_CODE_CLOSE:
            execute_close(cmd);
            return;
        case TFS_OP_CODE_WRITE:
            execute_write(cmd);
            return;
        case TFS_OP_CODE_READ:
            execute_read(cmd);
            return;
        case TFS_OP_CODE_SHUTDOWN_AFTER_ALL_CLOSED:
            execute_shutdown(cmd);
            return;
        default:
            free(cmd);
            fprintf(stderr, "Received invalid opcode with value: %d\n", opcode);
            return;
    }
}

execute_mount(command *cmd) {
    fprintf(stderr, "Mounting in thread %d with pipename\n", cmd->session_id, cmd->fifo_name);
    // technically write_fd is shared memory because it can be used by 
    // the main thread when it calls terminate_session: however,
    // there is no need to protect it with the mutex because, since the client is sequential,
    // it is impossible for this thread to be executing a command and for the main thread to be receiving
    // another command with the session id (for correct clients, and they are correct according to the 
    // project description. I still protect the access because it is shared memory and I don't want to lose points
    int ret_code;
    
    if ((ret_code = pthread_mutex_lock(&session_mutex[cmd->session_id])) != 0) {
        fprintf(stderr, "Error locking mutex for session %d in mount: %s\n", cmd->session_id, strerror(ret_code));
        terminate_session(cmd->session_id);
        free(cmd);
        return;
    }
    while ((write_fds[cmd->session_id] = open(cmd->fifo_name, O_WRONLY)) == -1) {
        if (errno == EINTR)
            continue;
        perror("Error opening client's fifo in mount");
        if ((ret_code = pthread_mutex_unlock(&session_mutex[cmd->session_id])) != 0) {
            fprintf(stderr, "Error unlocking mutex for session %d in mount: %s\n", cmd->session_id, strerror(ret_code));
        }
        terminate_session(cmd->session_id);
        free(cmd);
        return;
    }
    fprintf(stderr, "We opened the client's pipe for writing\n");
    if (write_all(write_fds[cmd->session_id], &cmd->session_id, sizeof(int)) != 0) {
        perror("Error writing to client's fifo in mount");
        if ((ret_code = pthread_mutex_unlock(&session_mutex[cmd->session_id])) != 0) {
            fprintf(stderr, "Error unlocking mutex for session %d in mount: %s\n", cmd->session_id, strerror(ret_code));
        }
        terminate_session(cmd->session_id);
        free(cmd);
        return;
    }
    if ((ret_code = pthread_mutex_unlock(&session_mutex[cmd->session_id])) != 0) {
        fprintf(stderr, "Error unlocking mutex for session %d in mount: %s\n", cmd->session_id, strerror(ret_code));
        terminate_session(cmd->session_id);
        free(cmd);
        return;
    }
    free(cmd);
    fprintf(stderr, "We wrote to the client the session id\n");
}

execute_unmount(command *cmd) {
    fprintf(stderr, "We're going to unmount in session %d\n", cmd->session_id);
    ret_code = free_session_id(session_id); 
    printf("We got that ret_code %d\n", ret_code);
    if (write_all(write_fd, &ret_code, sizeof(int)) != 0) {
        perror("Error writing to client's fifo in unmount");
        close(write_fd);
        return NULL;
    }
    fprintf(stderr, "We wrote that ret_code\n");
    if (close(write_fd) != 0) {
        perror("Error closing write file descriptor in unmount");
        return NULL;
    }
    // now the client
    printf("We're done unmounting\n");
}
void *worker(void *arg) {
    int *arg_int = (int *) arg;
    int session_id = *arg_int, send_id, recv_id;
    command *cmd;
    free(arg_int);
    char opcode;
    int ret_code, ret_error;
    int flags, fhandle;
    char pipe_name[FIFO_NAME_SIZE+1], file_name[MAX_FILE_NAME+1], rw_buffer[PIPE_BUF];
    int write_fd = -1;
    size_t len;
    while (1) {
        if ((ret_error = read_from_buffer(&cmd, sizeof(cmd), session_id) != 0)) {
            fprintf(stderr, "Error reading from PC buffer in session %d: %s\n", session_id);
            terminate_session(session_id);
            continue;
        }
        execute_command(cmd);  
        else if (opcode == TFS_OP_CODE_UNMOUNT) {
        }
        else if (opcode == TFS_OP_CODE_OPEN) {
            printf("Going to open a file\n");
            if ((ret_error = read_from_buffer(&recv_id, sizeof(recv_id), session_id)) != 0) {
                fprintf(stderr, "Error reading session_id from buffer in open session %d: %s\n", session_id, strerror(ret_error));
                return NULL;
            }
            printf("We received the session_id from the client: %d\n", recv_id);
            if ((ret_error = read_from_buffer(&file_name, MAX_FILE_NAME, session_id)) != 0) {
                fprintf(stderr, "Error reading file name from buffer in open session %d: %s\n", session_id, strerror(ret_error));
                return NULL;
            }
            file_name[MAX_FILE_NAME] = '\0'; // we need to make sure the last character is a \0: inside the filesystem strlen is called with the filename
                                             // the file's name inside the filesystem has MAX_FILE_NAME characters, including the '\0': however, the first
                                             // character of all names is not stored, so we can send MAX_FILE_NAME+1 bytes (including \0), and this way
                                             // we don't need to count on the client having sent a '\0', and we send all the bytes that can be used
                                             // it is actually safe to send any amount of bytes as long as there is a
                                             // '\0' due to the way the function add_dir_entry makes sure that the last character is a '\0'
            printf("We've read the filename it is :%s\n", file_name);
            if ((ret_error = read_from_buffer(&flags, sizeof(flags), session_id)) != 0) {
                fprintf(stderr, "Error reading flags from buffer in open session %d: %s\n", session_id, strerror(ret_error));
                return NULL;
            }
            printf("We've read the flags and they are %d\n", flags);
            ret_code = tfs_open(file_name, flags);
            printf("We opened the file with ret_code %d\n", ret_code);
            if (write_all(write_fd, &ret_code, sizeof(int)) != 0) {
                perror("Error writing to client's fifo in open");
                close(write_fd);
                return NULL;
            }
            printf("We wrote the ret_code to client through the pipe\n");
        }
        else if (opcode == TFS_OP_CODE_CLOSE) {
            printf("Going to close a file\n");
            if ((ret_error = read_from_buffer(&recv_id, sizeof(recv_id), session_id)) != 0) {
                fprintf(stderr, "Error reading session_id from buffer in close session %d: %s\n", session_id, strerror(ret_error));
                return NULL;
            }
            printf("We got the session_id %d\n", recv_id);
            if ((ret_error = read_from_buffer(&fhandle, sizeof(fhandle), session_id)) != 0) {
                fprintf(stderr, "Error reading fhandle from buffer in close session %d: %s\n", session_id, strerror(ret_error));
                return NULL;
            }
            printf("We got the file handle: %d\n", fhandle);
            ret_code = tfs_close(fhandle);
            printf("We got the ret_code %d\n", ret_code);
            if (write_all(write_fd, &ret_code, sizeof(int)) != 0) {
                perror("Error writing to client's fifo in unmount");
                close(write_fd); // should close all of them
                return NULL;
            }
            printf("We wrote to the mofucking client\n");
        }
        else if (opcode == TFS_OP_CODE_WRITE) {
            printf("WE gon write\n");
            if ((ret_error = read_from_buffer(&recv_id, sizeof(recv_id), session_id)) != 0) {
                fprintf(stderr, "Error reading session_id from buffer in write session %d: %s\n", session_id, strerror(ret_error));
                return NULL;
            }
            printf("The session_id is %d\n", recv_id);
            if ((ret_error = read_from_buffer(&fhandle, sizeof(fhandle), session_id)) != 0) {
                fprintf(stderr, "Error reading fhandle from buffer in write session %d: %s\n", session_id, strerror(ret_error));
                return NULL;
            }
            printf("The fhandle was read and is %d\n", fhandle);
            if ((ret_error = read_from_buffer(&len, sizeof(len), session_id)) != 0) {
                fprintf(stderr, "Error reading len from buffer in write session %d: %s\n", session_id, strerror(ret_error));
                return NULL;
            }
            printf("The len was read and is %zu\n", len);
            if (len > 0 && (ret_error = read_from_buffer(rw_buffer, len, session_id)) != 0) {
                fprintf(stderr, "Error reading bytes to write from buffer in write session %d: %s\n", session_id, strerror(ret_error));
                close(write_fd); // should close all of them
                // write_fd = -1;
                return NULL;
            }
            printf("We read all the bytes from the buffer: %s\n", rw_buffer);
            // note that if len is 0 it doesn't matter what rw_buffer has
            ret_code = (int) tfs_write(fhandle, rw_buffer, len); // this is a weird cast: the communication protocol uses int but the fs uses ssize_t
            printf("We wrote into tfs with ret_code %d\n", ret_code); 
            if (write_all(write_fd, &ret_code, sizeof(int)) != 0) {
                perror("Error writing to client's fifo in unmount");
                close(write_fd); // should close all of them
                // write_fd = -1;
                return NULL;
            }
            printf("We wrote the ret_code into the client's pipe");
        }
        else if (opcode == TFS_OP_CODE_READ) {
            printf("We about to read \n");
            if ((ret_error = read_from_buffer(&recv_id, sizeof(recv_id), session_id)) != 0) {
                fprintf(stderr, "Error reading session_id from buffer in read session %d: %s\n", session_id, strerror(ret_error));
                close(write_fd);
                return NULL;
            }
            printf("We read from the pc buffer the session_id: %d\n", recv_id);
            if ((ret_error = read_from_buffer(&fhandle, sizeof(fhandle), session_id)) != 0) {
                fprintf(stderr, "Error reading fhandle from buffer in read session %d: %s\n", session_id, strerror(ret_error));
                close(write_fd);
                return NULL;
            }
            printf("We read dat fhandle: %d\n", fhandle);
            if ((ret_error = read_from_buffer(&len, sizeof(len), session_id)) != 0) {
                fprintf(stderr, "Error reading len from buffer in read session %d: %s\n", session_id, strerror(ret_error));
                close(write_fd);
                return NULL;
            }
            printf("We read the len: %zu\n", len);
            ret_code = (int) tfs_read(fhandle, rw_buffer, len); // weird cast
            printf("We read from tfs: %d\n", ret_code);
            if (write_all(write_fd, &ret_code, sizeof(int)) != 0) {
                perror("Error writing return code to client's fifo in read");
                close(write_fd); // should close all of them
                return NULL;
            }
            printf("We wrote into the client's pipe the ret_code\n");
            if (ret_code > 0 && write_all(write_fd, rw_buffer, (size_t) ret_code) != 0) {
                perror("Error writing bytes to client's fifo in read");
                close(write_fd); // should close all of them
                return NULL;
            }
            printf("We wrote %s into the client's pipe\n", rw_buffer);
        }
        else if (opcode == TFS_OP_CODE_SHUTDOWN_AFTER_ALL_CLOSED) {
            if ((ret_error = read_from_buffer(&recv_id, sizeof(recv_id), session_id)) != 0) {
                fprintf(stderr, "Error reading session_id from buffer in shutdown session %d: %s\n", session_id, strerror(ret_error));
                close(write_fd);
                return NULL;
            }
            ret_code = tfs_destroy_after_all_closed();
            if (write_all(write_fd, &ret_code, sizeof(ret_code)) != 0) {
                perror("Error writing to client's fifo in shutdown");
                close(write_fd); // should close all of them
                return NULL;
            }
            // if the filesystem was destroyed successfully we exit the program
            if (ret_code == 0) {
                // it doesn't really matter if we get an error here: we are going to exit the program
                // anyway
                if (pthread_mutex_lock(&exit_mutex) != 0)
                    exit(EXIT_FAILURE);
                exit(EXIT_SUCCESS);
            }
        }
        else {
            fprintf(stderr, "Received incorrect opcode\n");
            return NULL; 
        }
    }
    return NULL; // will never get here
}
