#include "common/comms.h"
#include "operations.h"
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define S (20)
#define PC_BUF_SIZE                                                            \
  (8) // chosen because this way it can fit an entire pointer in a 64 bit
      // architecture

typedef struct command {
  char opcode;
  int session_id;
  int fhandle;
  int flags;
  size_t len;
  char file_name[MAX_FILE_NAME + 1];
  char fifo_name[FIFO_NAME_SIZE + 1];
  char rw_buffer[PIPE_BUF]; // this buffer could be dinamically allocated as
                            // well, which would make more sense in an
                            // environment with more stern memory constraints. I
                            // chose not to do that because it was extra trouble
                            // for little reward. I could have also only used
                            // one buffer but I kept it like this to logically
                            // separate the buffer for the filename and the fifo
                            // name and the buffer for the reads/writes: again,
                            // we have plenty of memory
} command;

pthread_mutex_t exit_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t session_mutex[S] = {
    PTHREAD_MUTEX_INITIALIZER}; // this mutex will cover the prodptr, consptr,
                                // count and write_fd of the session using a
                                // separate mutex for the write fd would
                                // logically make some sense but in the end I
                                // thought it didn't matter
pthread_mutex_t free_session_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t can_write[S] = {PTHREAD_COND_INITIALIZER};
pthread_cond_t can_read[S] = {PTHREAD_COND_INITIALIZER};
size_t prodptrs[S] = {0};
size_t consptrs[S] = {0};
size_t counts[S] = {0};
char pc_buffers[S][PC_BUF_SIZE];
char taken_session[S] = {0};
int write_fds[S] = {-1};

void *worker(void *arg);
void parse_command(int read_fd, char opcode);
void safe_exit(int exit_code);

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
  if (mkfifo(pipename, 0777) !=
      0) { // stupid permissions, taken from the lecture slides
    perror("Error creating named pipe");
    return -1;
  }

  int aux_read_fd, read_fd;
  while ((read_fd = open(pipename, O_RDONLY)) == -1) {
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
  int ret_code, *worker_id;
  char opcode;
  pthread_t workers[S];
  for (int i = 0; i < S; i++) {
    worker_id = malloc(sizeof(int));
    if (worker_id == NULL) {
      fprintf(stderr, "Error allocing argument for thread %d\n", i);
      safe_exit(EXIT_FAILURE);
    }
    *worker_id = i;
    if ((ret_code = pthread_create(&workers[i], NULL, worker,
                                   (void *)worker_id)) != 0) {
      fprintf(stderr, "Error creating thread: %s\n", strerror(ret_code));
      safe_exit(EXIT_FAILURE);
    }
    if ((ret_code = pthread_detach(workers[i])) != 0) {
      fprintf(stderr, "Error detaching thread: %s\n", strerror(ret_code));
      safe_exit(EXIT_FAILURE);
    }
  }

  while (1) {
    if ((read_ret_code = read(read_fd, &opcode, 1)) < 0 && errno != EINTR) {
      perror("Main thread: Error reading opcode");
      unlink(pipename);
      close(read_fd);
      safe_exit(EXIT_FAILURE);
    } else if (errno == EINTR)
      continue;
    else if (read_ret_code ==
             0) { // this happens when there are no clients that have opened the
                  // file for writing (they all closed it already)
      while ((aux_read_fd = open(pipename, O_RDONLY)) == -1) {
        if (errno == EINTR)
          continue;
        perror(
            "Error opening server's FIFO for reading"); // we close the server
                                                        // here: not being able
                                                        // to open the FIFO is a
                                                        // catastrophic error
        close(read_fd);
        unlink(pipename);
        safe_exit(EXIT_FAILURE);
      } // wait here for someone to open
      if (close(aux_read_fd) != 0) {
        perror(
            "Error closing auxiliary read file descriptor"); // here I decided
                                                             // not to close the
                                                             // server: the
                                                             // other fd is
                                                             // still open and
                                                             // might be usable
      }
      continue;
    }
    parse_command(read_fd, opcode);
  }

  safe_exit(EXIT_SUCCESS);
}

bool valid_session_id(int session_id) {
  return (session_id > -1 && session_id < S);
}

bool active_session(int session_id) {
  bool active = false;
  int ret_code;
  if (valid_session_id(session_id)) {
    if ((ret_code = pthread_mutex_lock(&free_session_mutex)) != 0) {
      fprintf(stderr,
              "Error locking free_session_mutex in active_session_id(): %s\n",
              strerror(ret_code));
      safe_exit(EXIT_FAILURE);
    }
    active = taken_session[session_id];
    if ((ret_code = pthread_mutex_unlock(&free_session_mutex)) != 0) {
      fprintf(stderr,
              "Error unlocking free_session_mutex in active_session_id(): %s\n",
              strerror(ret_code));
      safe_exit(EXIT_FAILURE);
    }
  } else
    fprintf(stderr, "Invalid session_id sent to active_session_id: %d\n",
            session_id);
  return active;
}

int get_session_id() { // an int because session_id are ints
  int ret_code;
  if ((ret_code = pthread_mutex_lock(&free_session_mutex)) != 0) {
    fprintf(stderr, "Error locking free session mutex in get session id: %s\n",
            strerror(ret_code));
    safe_exit(EXIT_FAILURE);
  }
  for (int i = 0; i < S; i++) {
    if (!taken_session[i]) {
      taken_session[i] = 1;
      if ((ret_code = pthread_mutex_unlock(&free_session_mutex)) != 0) {
        fprintf(stderr,
                "Error unlocking free session mutex in get session id: %s\n",
                strerror(ret_code));
        safe_exit(EXIT_FAILURE);
      }
      return i;
    }
  }
  if ((ret_code = pthread_mutex_unlock(&free_session_mutex)) != 0) {
    fprintf(stderr,
            "Error unlocking free session mutex in get session id: %s\n",
            strerror(ret_code));
    safe_exit(EXIT_FAILURE);
  }
  return -1;
}

void safe_exit(int exit_code) {
  int ret_code;
  if ((ret_code = pthread_mutex_lock(&exit_mutex)) != 0) {
    fprintf(stderr, "Error locking exit mutex: %s\n", strerror(ret_code));
    exit(EXIT_FAILURE);
  }
  exit(exit_code); // I exit anyway to make sure the program crashes: not sure
                   // if I should, since exit is not MT safe. However, in my man
                   // page (Linux, not POSIX) none of the errors are likely to
                   // happen: EINVAL because the mutex is always properly
                   // initialized (with the macro), and EDEADLOCK because this
                   // is not recursive mutex
}

void _write_into_buffer_unsychronized(const void *src, size_t len,
                                      char *pc_buffer, size_t *prodptr,
                                      size_t *count) {
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

void _read_from_buffer_unsychronized(void *dest, size_t len, char *pc_buffer,
                                     size_t *consptr, size_t *count) {
  *count -= len; // right now this is not valid, but will be at the end of the
                 // function
  // if we do not need to go around
  if ((*consptr) + len <= PC_BUF_SIZE) {
    memcpy(dest, pc_buffer + (*consptr), len);
    *consptr += len;
    if ((*consptr) == PC_BUF_SIZE)
      *consptr = 0;
    return;
  }
  size_t to_read_from_end =
      PC_BUF_SIZE - (*consptr); // bytes to read from the end of the buffer
                                // before we circle back to the beginning
  size_t to_read_from_start =
      len -
      to_read_from_end; // bytes to read from the start after we circle back
  memcpy(
      dest, pc_buffer + (*consptr),
      to_read_from_end); // after this call, *consptr is 0 but was not updated
  memcpy(dest + to_read_from_end, pc_buffer, to_read_from_start);
  *consptr =
      to_read_from_start; // no need to check if it is PC_BUF_SIZE: since we
                          // read some from the end, at most it is the value of
                          // *consptr at the start of the function
}

int read_from_buffer(void *dest, size_t len, int session_id) {
  if (!valid_session_id(session_id)) {
    fprintf(stderr,
            "Tried to read from a buffer without a valid session_id : %d\n",
            session_id);
    return -1;
  }
  int ret_code = 0;
  size_t to_read, total_read = 0;
  if ((ret_code = pthread_mutex_lock(&session_mutex[session_id])) != 0) {
    fprintf(stderr, "Error locking mutex in read_from_buffer, session %d: %s\n",
            session_id, strerror(ret_code));
    safe_exit(EXIT_FAILURE);
  }
  // this is a bit nasty but it allows for the (very unlikely if not impossible)
  // chance that there are some bytes to be read but not enough to fill dest.
  while (len > 0) {
    while (counts[session_id] == 0) {
      if ((ret_code = pthread_cond_wait(&can_read[session_id],
                                        &session_mutex[session_id])) != 0) {
        fprintf(stderr, "Error waiting in read_from_buffer, session %d: %s\n",
                session_id, strerror(ret_code));
        safe_exit(EXIT_FAILURE);
      }
    }
    // we read only the necessary bytes, or the ones available if there are less
    // than the necessary
    to_read = counts[session_id] < len ? counts[session_id] : len;
    len -= to_read;
    _read_from_buffer_unsychronized(dest + total_read, to_read,
                                    pc_buffers[session_id],
                                    &consptrs[session_id], &counts[session_id]);
    total_read += to_read;
    if ((ret_code = pthread_cond_signal(&can_write[session_id])) != 0) {
      fprintf(stderr,
              "Error signalling ability to write to the producer consumer "
              "buffer of session %d: %s\n",
              session_id, strerror(ret_code));
      safe_exit(EXIT_FAILURE);
    }
  }
  if ((ret_code = pthread_mutex_unlock(&session_mutex[session_id])) != 0) {
    fprintf(stderr,
            "Error unlocking mutex in read_from_buffer, session %d: %s\n",
            session_id, strerror(ret_code));
    safe_exit(EXIT_FAILURE);
  }
  return 0;
}

int write_into_buffer(const void *src, size_t len, int session_id) {
  if (!valid_session_id(session_id)) {
    fprintf(stderr,
            "Tried to write into a pc buffer of an invalid session: %d\n",
            session_id);
    return -1;
  }
  int ret_code = 0;
  if ((ret_code = pthread_mutex_lock(&session_mutex[session_id])) != 0) {
    fprintf(stderr, "Error locking session %d mutex in write_into_buffer: %s\n",
            session_id, strerror(ret_code));
    safe_exit(EXIT_FAILURE);
  }
  size_t to_write, total_written = 0, available_bytes;
  while (len > 0) {
    while (counts[session_id] == PC_BUF_SIZE) {
      if ((ret_code = pthread_cond_wait(&can_write[session_id],
                                        &session_mutex[session_id])) != 0) {
        fprintf(stderr, "Error waiting to write in pc buffer session %d: %s\n",
                session_id, strerror(ret_code));
        safe_exit(EXIT_FAILURE);
      }
      // notice that if the client was not sequential, there would be a chance
      // that the session would no longer be active here. However with the
      // sequential client what will happen will be 1-receive command through
      // pipe, 2-send command to pc buffer, 3-read command to pc buffer 4-
      // handle command in worker thread and send response and back to 1. For
      // non sequential clients this code is wrong, and so is read_into_buffer,
      // because while someone was waiting another one could terminate the
      // session
    }
    available_bytes = PC_BUF_SIZE - counts[session_id];
    to_write = len > available_bytes ? available_bytes : len;
    _write_into_buffer_unsychronized(
        src + total_written, to_write, pc_buffers[session_id],
        &prodptrs[session_id], &counts[session_id]);
    total_written += to_write;
    len -= to_write;
    if ((ret_code = pthread_cond_signal(&can_read[session_id])) != 0) {
      fprintf(stderr, "Error signalling ability to read in session %d: %s\n",
              session_id, strerror(ret_code));
      safe_exit(EXIT_FAILURE);
    }
  }
  if ((ret_code = pthread_mutex_unlock(&session_mutex[session_id])) != 0) {
    fprintf(stderr,
            "Error unlocking mutex in write_into_buffer session %d: %s\n",
            session_id, strerror(ret_code));
    safe_exit(EXIT_FAILURE);
  }
  return 0;
}

void send_return_code(int session_id, int ret_code) {
  if (write_fds[session_id] != -1 &&
      write_all(write_fds[session_id], &ret_code, sizeof(int)) != 0) {
    fprintf(stderr,
            "Error writing to client's fifo when clearing session %d: %s\n",
            session_id, strerror(errno));
  }
}

void clear_session(int session_id, int send_code) {
  int ret_code;
  if (!valid_session_id(session_id)) {
    fprintf(stderr, "Invalid session_id sent to clear_session: %d\n",
            session_id);
    return;
  }
  if ((ret_code = pthread_mutex_lock(&session_mutex[session_id])) != 0) {
    fprintf(stderr, "Error locking session %d mutex in free_session_id: %s\n",
            session_id, strerror(ret_code));
    send_return_code(session_id, -1); // even though we failed to lock the
                                      // mutex, we try to send the error message
    safe_exit(EXIT_FAILURE);
  }
  // clear pc buffer
  consptrs[session_id] = 0;
  prodptrs[session_id] = 0;
  counts[session_id] = 0;
  // send final code to client
  send_return_code(session_id, send_code);
  if (write_fds[session_id] != -1 &&
      close(write_fds[session_id]) != 0) // in Linux, ignore EINTR in close
    fprintf(stderr, "Error closing file descriptor %d for session %d: %s\n",
            write_fds[session_id], session_id, strerror(errno));
  write_fds[session_id] = -1;
  if ((ret_code = pthread_mutex_unlock(&session_mutex[session_id])) != 0) {
    fprintf(stderr, "Error unlocking session %d mutex in free_session_id: %s\n",
            session_id, strerror(ret_code));
    safe_exit(EXIT_FAILURE);
  }
}

void free_session_id(int session_id) {
  int ret_code;
  if ((ret_code = pthread_mutex_lock(&free_session_mutex)) != 0) {
    fprintf(stderr, "Error locking free session mutex in free_session_id: %s\n",
            strerror(ret_code));
    safe_exit(EXIT_FAILURE);
  }
  taken_session[session_id] = 0;
  if ((ret_code = pthread_mutex_unlock(&free_session_mutex)) != 0) {
    fprintf(stderr,
            "Error unlocking free session mutex in free_session_id: %s\n",
            strerror(ret_code));
    safe_exit(EXIT_FAILURE);
  }
}

// this function can be called by the receiver thread and the worker thread, but
// not at the same time because the client is sequential so the receiver thread
// will never receive a new command for the same session while the worker thread
// is working
void terminate_session(int session_id, int send_code) {
  clear_session(session_id, send_code);
  free_session_id(session_id);
}

void parse_mount(int read_fd, command *cmd) {
  int write_fd;
  int session_id;
  if (read_all(read_fd, cmd->fifo_name, FIFO_NAME_SIZE) != 0) {
    free(cmd);
    perror(
        "Error reading fifo's name in mount"); // we continue execution: this
                                               // might be problematic for some
                                               // errors in read... I figure
                                               // they will then appear when we
                                               // read the next opcode
    return;
  }
  cmd->fifo_name[FIFO_NAME_SIZE] = '\0';
  session_id = get_session_id();
  // I always call perror: note that there is a chance for example in write_all
  // that errno was not updated. This is true in lots of places around the code,
  // but I figure that's ok
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
  } else {
    cmd->session_id = session_id;
    if (write_into_buffer(&cmd, sizeof(cmd), session_id) != 0) {
      fprintf(stderr,
              "Error writing to producer consumer buffer in session %d mount\n",
              session_id);
      free(cmd);
      terminate_session(session_id, -1);
    }
  }
}

void parse_unmount(int read_fd, command *cmd) {
  if (read_all(read_fd, &(cmd->session_id), sizeof(int)) != 0) {
    perror("Error reading session id in unmount");
    free(cmd);
    return;
  }
  if (!active_session(cmd->session_id)) {
    fprintf(stderr, "Received unmount command for inactive session %d\n",
            cmd->session_id);
    free(cmd);
    return;
  }
  if ((write_into_buffer(&cmd, sizeof(cmd), cmd->session_id)) != 0) {
    fprintf(stderr,
            "Error writing to producer consumer buffer in session %d unmount\n",
            cmd->session_id);
    terminate_session(cmd->session_id, -1);
    free(cmd);
  }
}

void parse_open(int read_fd, command *cmd) {
  if (read_all(read_fd, &cmd->session_id, sizeof(int)) != 0) {
    perror("Error reading session id in open");
    free(cmd);
    return;
  }
  if (!active_session(cmd->session_id)) {
    fprintf(stderr, "Received open command for inactive session %d\n",
            cmd->session_id);
    free(cmd);
    return;
  }
  if (read_all(read_fd, cmd->file_name, MAX_FILE_NAME) != 0) {
    fprintf(stderr, "Error reading filename in open session %d: %s\n",
            cmd->session_id, strerror(errno));
    terminate_session(cmd->session_id, -1);
    free(cmd);
    return;
  }
  cmd->file_name[MAX_FILE_NAME] = '\0';
  if (read_all(read_fd, &cmd->flags, sizeof(int)) != 0) {
    fprintf(stderr, "Error reading flags in open session %d: %s\n",
            cmd->session_id, strerror(errno));
    terminate_session(cmd->session_id, -1);
    free(cmd);
    return;
  }
  if (write_into_buffer(&cmd, sizeof(cmd), cmd->session_id) != 0) {
    fprintf(stderr,
            "Error writing to producer consumer buffer in session %d open\n",
            cmd->session_id);
    terminate_session(cmd->session_id, -1);
    free(cmd);
  }
}

void parse_close(int read_fd, command *cmd) {
  if (read_all(read_fd, &cmd->session_id, sizeof(int)) != 0) {
    perror("Error reading session id in close");
    free(cmd);
    return;
  }
  if (!active_session(cmd->session_id)) {
    fprintf(stderr, "Received close command for inactive session %d\n",
            cmd->session_id);
    free(cmd);
    return;
  }
  if (read_all(read_fd, &cmd->fhandle, sizeof(int)) != 0) {
    fprintf(stderr, "Error reading fhandle in open session %d: %s\n",
            cmd->session_id, strerror(errno));
    terminate_session(cmd->session_id, -1);
    free(cmd);
    return;
  }
  if (write_into_buffer(&cmd, sizeof(cmd), cmd->session_id) != 0) {
    fprintf(stderr,
            "Error writing to producer consumer buffer in session %d close\n",
            cmd->session_id);
    terminate_session(cmd->session_id, -1);
    free(cmd);
  }
}

void parse_write(int read_fd, command *cmd) {
  if (read_all(read_fd, &cmd->session_id, sizeof(int)) != 0) {
    perror("Error reading session id in write");
    free(cmd);
    return;
  }
  if (!active_session(cmd->session_id)) {
    fprintf(stderr, "Received write command for inactive session %d\n",
            cmd->session_id);
    free(cmd);
    return;
  }
  if (read_all(read_fd, &cmd->fhandle, sizeof(int)) != 0) {
    fprintf(stderr, "Error reading fhandle in write session %d: %s\n",
            cmd->session_id, strerror(errno));
    terminate_session(cmd->session_id, -1);
    free(cmd);
    return;
  }
  if (read_all(read_fd, &cmd->len, sizeof(size_t)) != 0) {
    fprintf(stderr, "Error reading len in write session %d: %s\n",
            cmd->session_id, strerror(errno));
    terminate_session(cmd->session_id, -1);
    free(cmd);
    return;
  }
  if (cmd->len > 0 && read_all(read_fd, cmd->rw_buffer, cmd->len) != 0) {
    fprintf(stderr, "Error reading bytes to write in write session %d: %s\n",
            cmd->session_id, strerror(errno));
    terminate_session(cmd->session_id, -1);
    free(cmd);
    return;
  }
  if (write_into_buffer(&cmd, sizeof(cmd), cmd->session_id) != 0) {
    fprintf(stderr,
            "Error writing to producer consumer buffer in session %d write\n",
            cmd->session_id);
    terminate_session(cmd->session_id, -1);
    free(cmd);
  }
}

void parse_read(int read_fd, command *cmd) {
  if (read_all(read_fd, &cmd->session_id, sizeof(int)) != 0) {
    perror("Error reading session id in read");
    free(cmd);
    return;
  }
  if (!active_session(cmd->session_id)) {
    fprintf(stderr, "Received read command for inactive session %d\n",
            cmd->session_id);
    free(cmd);
    return;
  }
  if (read_all(read_fd, &cmd->fhandle, sizeof(int)) != 0) {
    fprintf(stderr, "Error reading fhandle in read session %d: %s\n",
            cmd->session_id, strerror(errno));
    terminate_session(cmd->session_id, -1);
    free(cmd);
    return;
  }
  if (read_all(read_fd, &cmd->len, sizeof(size_t)) != 0) {
    fprintf(stderr, "Error reading len in read session %d: %s\n",
            cmd->session_id, strerror(errno));
    terminate_session(cmd->session_id, -1);
    free(cmd);
    return;
  }
  if (write_into_buffer(&cmd, sizeof(cmd), cmd->session_id) != 0) {
    fprintf(stderr,
            "Error writing to producer consumer buffer in session %d read\n",
            cmd->session_id);
    terminate_session(cmd->session_id, -1);
    free(cmd);
  }
}

void parse_shutdown(int read_fd, command *cmd) {
  if (read_all(read_fd, &cmd->session_id, sizeof(int)) != 0) {
    perror("Error reading session id in shutdown after all closed");
    free(cmd);
    return;
  }
  if (!active_session(cmd->session_id)) {
    fprintf(stderr, "Received shutdown command for inactive session %d\n",
            cmd->session_id);
    free(cmd);
    return;
  }
  if (write_into_buffer(&cmd, sizeof(cmd), cmd->session_id) != 0) {
    fprintf(stderr,
            "Error writing to producer consumer buffer in session %d unmount\n",
            cmd->session_id);
    terminate_session(cmd->session_id, -1);
    free(cmd);
  }
}

void parse_command(int read_fd, char opcode) {
  command *cmd = malloc(
      sizeof(command)); // I allocate it in this function for code reuse: every
                        // function would have to call malloc and check for
                        // errors, and this way it's all done in one place. The
                        // only tradeoff is I still malloc for invalid opcodes
  if (cmd == NULL) {
    fprintf(stderr, "Error mallocing command\n");
    safe_exit(EXIT_FAILURE);
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

void execute_mount(command *cmd) {
  fprintf(stderr, "Mounting in thread %d with pipename %s\n", cmd->session_id,
          cmd->fifo_name);
  // technically write_fd is shared memory because it can be used by
  // the main thread when it calls terminate_session: however,
  // there is no need to protect it with the mutex IMO because, since the client
  // is sequential, it is impossible for this thread to be executing a command
  // and for the main thread to be receiving another command with the same
  // session id (for correct clients, and they are correct according to the
  // project description). I still protect the access because it is shared
  // memory and I don't want to lose points
  int ret_code;

  if ((ret_code = pthread_mutex_lock(&session_mutex[cmd->session_id])) != 0) {
    fprintf(stderr, "Error locking mutex for session %d in mount: %s\n",
            cmd->session_id, strerror(ret_code));
    terminate_session(cmd->session_id, -1);
    safe_exit(EXIT_FAILURE);
  }
  while ((write_fds[cmd->session_id] = open(cmd->fifo_name, O_WRONLY)) == -1) {
    if (errno == EINTR)
      continue;
    perror("Error opening client's fifo in mount");
    if ((ret_code = pthread_mutex_unlock(&session_mutex[cmd->session_id])) !=
        0) {
      fprintf(stderr, "Error unlocking mutex for session %d in mount: %s\n",
              cmd->session_id, strerror(ret_code));
      safe_exit(EXIT_FAILURE);
    }
    terminate_session(cmd->session_id, -1);
    return;
  }
  fprintf(stderr, "We opened the client's pipe for writing\n");
  if (write_all(write_fds[cmd->session_id], &cmd->session_id, sizeof(int)) !=
      0) {
    perror("Error writing to client's fifo in mount");
    if ((ret_code = pthread_mutex_unlock(&session_mutex[cmd->session_id])) !=
        0) {
      fprintf(stderr, "Error unlocking mutex for session %d in mount: %s\n",
              cmd->session_id, strerror(ret_code));
      terminate_session(cmd->session_id, -1);
      safe_exit(EXIT_FAILURE);
    }
    terminate_session(cmd->session_id, -1);
    return;
  }
  if ((ret_code = pthread_mutex_unlock(&session_mutex[cmd->session_id])) != 0) {
    fprintf(stderr, "Error unlocking mutex for session %d in mount: %s\n",
            cmd->session_id, strerror(ret_code));
    safe_exit(EXIT_FAILURE);
  }
  fprintf(stderr, "We wrote to the client the session id\n");
}

void execute_unmount(command *cmd) {
  fprintf(stderr, "We're going to unmount in session %d\n", cmd->session_id);
  terminate_session(cmd->session_id, 0);
  fprintf(stderr, "We're done unmounting\n");
}

void execute_open(command *cmd) {
  int ret_code, aux_ret_code;
  fprintf(stderr, "Going to open a file named %s in session %d\n",
          cmd->file_name, cmd->session_id);
  ret_code = tfs_open(cmd->file_name, cmd->flags);
  fprintf(stderr, "We opened the file %s in session %d with ret_code %d\n",
          cmd->file_name, cmd->session_id, ret_code);
  if ((aux_ret_code = pthread_mutex_lock(&session_mutex[cmd->session_id])) !=
      0) {
    fprintf(stderr, "Error locking mutex in session %d open: %s\n",
            cmd->session_id, strerror(aux_ret_code));
    terminate_session(cmd->session_id, -1);
    safe_exit(EXIT_FAILURE);
  }
  if (write_all(write_fds[cmd->session_id], &ret_code, sizeof(int)) != 0) {
    perror("Error writing to client's fifo in open");
    if ((ret_code = pthread_mutex_unlock(&session_mutex[cmd->session_id])) !=
        0) {
      fprintf(stderr, "Error unlocking mutex for session %d in open: %s\n",
              cmd->session_id, strerror(ret_code));
    }
    terminate_session(cmd->session_id, -1);
    return;
  }
  if ((ret_code = pthread_mutex_unlock(&session_mutex[cmd->session_id])) != 0) {
    fprintf(stderr, "Error unlocking mutex for session %d in open: %s\n",
            cmd->session_id, strerror(ret_code));
    safe_exit(EXIT_FAILURE);
  }
  fprintf(stderr,
          "We wrote the ret_code of open to client through the pipe of session "
          "%d\n",
          cmd->session_id);
}

void execute_close(command *cmd) {
  int ret_code, aux_ret_code;
  fprintf(stderr, "Going to close a file with handle %d in session %d\n",
          cmd->fhandle, cmd->session_id);
  ret_code = tfs_close(cmd->fhandle);
  fprintf(stderr, "We got the ret_code %d for the close in session %d\n",
          ret_code, cmd->session_id);
  if ((aux_ret_code = pthread_mutex_lock(&session_mutex[cmd->session_id])) !=
      0) {
    fprintf(stderr, "Error locking mutex in session %d close: %s\n",
            cmd->session_id, strerror(aux_ret_code));
    terminate_session(cmd->session_id, -1);
    safe_exit(EXIT_FAILURE);
  }
  if (write_all(write_fds[cmd->session_id], &ret_code, sizeof(int)) != 0) {
    perror("Error writing to client's fifo in close");
    if ((ret_code = pthread_mutex_unlock(&session_mutex[cmd->session_id])) !=
        0) {
      fprintf(stderr, "Error unlocking mutex for session %d in close: %s\n",
              cmd->session_id, strerror(ret_code));
    }
    terminate_session(cmd->session_id, -1);
    return;
  }
  if ((ret_code = pthread_mutex_unlock(&session_mutex[cmd->session_id])) != 0) {
    fprintf(stderr, "Error unlocking mutex for session %d in close: %s\n",
            cmd->session_id, strerror(ret_code));
    safe_exit(EXIT_FAILURE);
  }
  fprintf(stderr,
          "We wrote the ret_code of the close through the pipe of session %d\n",
          cmd->session_id);
}

void execute_write(command *cmd) {
  int ret_code, aux_ret_code;
  fprintf(
      stderr,
      "We are going to try to write %zu bytes in fhandle %d in session %d\n",
      cmd->len, cmd->fhandle, cmd->session_id);
  ret_code =
      (int)tfs_write(cmd->fhandle, cmd->rw_buffer,
                     cmd->len); // this is a weird cast: the communication
                                // protocol uses int but the fs uses ssize_t
  fprintf(stderr,
          "We wrote into tfs with ret_code %d in session %d and fhandle %d\n",
          ret_code, cmd->session_id, cmd->fhandle);
  if ((aux_ret_code = pthread_mutex_lock(&session_mutex[cmd->session_id])) !=
      0) {
    fprintf(stderr, "Error locking mutex in session %d write: %s\n",
            cmd->session_id, strerror(aux_ret_code));
    terminate_session(cmd->session_id, -1);
    safe_exit(EXIT_FAILURE);
  }
  if (write_all(write_fds[cmd->session_id], &ret_code, sizeof(int)) != 0) {
    perror("Error writing to client's fifo in write");
    if ((ret_code = pthread_mutex_unlock(&session_mutex[cmd->session_id])) !=
        0) {
      fprintf(stderr, "Error unlocking mutex for session %d in write: %s\n",
              cmd->session_id, strerror(ret_code));
    }
    terminate_session(cmd->session_id, -1);
    return;
  }
  if ((ret_code = pthread_mutex_unlock(&session_mutex[cmd->session_id])) != 0) {
    fprintf(stderr, "Error unlocking mutex for session %d in write: %s\n",
            cmd->session_id, strerror(ret_code));
    safe_exit(EXIT_FAILURE);
  }
  fprintf(
      stderr,
      "We wrote the ret_code of a write into the client's pipe of session %d\n",
      cmd->session_id);
}

void execute_read(command *cmd) {
  int ret_code, aux_ret_code;
  fprintf(
      stderr,
      "We are going to try to read %zu bytes from fhandle %d in session %d\n",
      cmd->len, cmd->fhandle, cmd->session_id);
  ret_code =
      (int)tfs_read(cmd->fhandle, cmd->rw_buffer, cmd->len); // weird cast
  fprintf(stderr, "We read from tfs session %d with return code: %d\n",
          cmd->session_id, ret_code);
  if ((aux_ret_code = pthread_mutex_lock(&session_mutex[cmd->session_id])) !=
      0) {
    fprintf(stderr, "Error locking mutex in session %d read: %s\n",
            cmd->session_id, strerror(aux_ret_code));
    terminate_session(cmd->session_id, -1);
    safe_exit(EXIT_FAILURE);
  }
  if (write_all(write_fds[cmd->session_id], &ret_code, sizeof(int)) != 0) {
    perror("Error writing return code to client's fifo in read");
    if ((ret_code = pthread_mutex_unlock(&session_mutex[cmd->session_id])) !=
        0) {
      fprintf(stderr, "Error unlocking mutex for session %d in read: %s\n",
              cmd->session_id, strerror(ret_code));
      safe_exit(EXIT_FAILURE);
    }
    terminate_session(cmd->session_id, -1);
    return;
  }
  fprintf(stderr,
          "We wrote into the client's pipe the ret_code in session %d read\n",
          cmd->session_id);
  if (ret_code > 0 && write_all(write_fds[cmd->session_id], cmd->rw_buffer,
                                (size_t)ret_code) != 0) {
    perror("Error writing bytes to client's fifo in read");
    if ((ret_code = pthread_mutex_unlock(&session_mutex[cmd->session_id])) !=
        0) {
      fprintf(stderr, "Error unlocking mutex for session %d in read: %s\n",
              cmd->session_id, strerror(ret_code));
      safe_exit(EXIT_FAILURE);
    }
    terminate_session(cmd->session_id,
                      -1); // here it doesn't make much sense to send the -1 but
                           // it won't hurt I guess
    return;
  }
  if ((ret_code = pthread_mutex_unlock(&session_mutex[cmd->session_id])) != 0) {
    fprintf(stderr, "Error unlocking mutex for session %d in write: %s\n",
            cmd->session_id, strerror(ret_code));
    safe_exit(EXIT_FAILURE);
  }
  fprintf(stderr, "We wrote all the bytes in session %d\n", cmd->session_id);
}

void execute_shutdown(command *cmd) {
  int aux_ret_code;
  int ret_code = tfs_destroy_after_all_closed();
  if ((aux_ret_code = pthread_mutex_lock(&session_mutex[cmd->session_id])) !=
      0) {
    fprintf(stderr, "Error locking mutex in session %d shutdown: %s\n",
            cmd->session_id, strerror(aux_ret_code));
    terminate_session(cmd->session_id, -1);
    safe_exit(EXIT_FAILURE);
  }
  if (write_all(write_fds[cmd->session_id], &ret_code, sizeof(ret_code)) != 0) {
    perror("Error writing to client's fifo in shutdown");
    if ((aux_ret_code =
             pthread_mutex_unlock(&session_mutex[cmd->session_id])) != 0) {
      fprintf(stderr, "Error unlocking mutex for session %d in read: %s\n",
              cmd->session_id, strerror(aux_ret_code));
      safe_exit(EXIT_FAILURE);
    }
    terminate_session(cmd->session_id, -1);
    return;
  }
  if ((aux_ret_code = pthread_mutex_unlock(&session_mutex[cmd->session_id])) !=
      0) {
    fprintf(stderr, "Error unlocking mutex for session %d in shutdown: %s\n",
            cmd->session_id, strerror(aux_ret_code));
    safe_exit(EXIT_FAILURE);
  }
  // if the filesystem was destroyed successfully we exit the program
  if (ret_code == 0) {
    fprintf(stderr, "Going to destroy the file system ordered by session %d\n",
            cmd->session_id);
    free(cmd);
    safe_exit(EXIT_SUCCESS);
  }
  fprintf(stderr,
          "There was an error destroying the file system in session %d\n",
          cmd->session_id);
}

void execute_command(command *cmd) {
  switch (cmd->opcode) {
  case TFS_OP_CODE_MOUNT:
    execute_mount(cmd);
    break;
  case TFS_OP_CODE_UNMOUNT:
    execute_unmount(cmd);
    break;
  case TFS_OP_CODE_OPEN:
    execute_open(cmd);
    break;
  case TFS_OP_CODE_CLOSE:
    execute_close(cmd);
    break;
  case TFS_OP_CODE_WRITE:
    execute_write(cmd);
    break;
  case TFS_OP_CODE_READ:
    execute_read(cmd);
    break;
  case TFS_OP_CODE_SHUTDOWN_AFTER_ALL_CLOSED:
    execute_shutdown(cmd);
    break;
  default:
    fprintf(stderr, "Received invalid opcode with value: %d\n", cmd->opcode);
    break;
  }
  free(cmd);
}

void *worker(void *arg) {
  int *arg_int = (int *)arg;
  int session_id = *arg_int;
  command *cmd;
  free(arg_int);
  while (1) {
    if (read_from_buffer(&cmd, sizeof(cmd), session_id) != 0) {
      fprintf(stderr, "Error reading from PC buffer in session %d\n",
              session_id);
      terminate_session(session_id, -1);
      continue;
    }
    execute_command(cmd);
  }
  return NULL; // will never get here
}
