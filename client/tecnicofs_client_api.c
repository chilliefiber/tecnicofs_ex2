#include "tecnicofs_client_api.h"

int write_fd; // fd of server's FIFO, open for writes only
int read_fd; // fd of this client's FIFO, open for reads only

int tfs_mount(char const *client_pipe_path, char const *server_pipe_path) {
    if (mkfifo(client_pipe_path, 0777) != 0) 
        return -1;

    if ((write_fd = open(server_pipe_path, O_WRONLY)) != 0) {
        unlink(client_pipe_path);
        return -1;
    }

    char msg[1 + FIFO_NAME_SIZE];
    msg[0] = TFS_OP_CODE_MOUNT;
    strncpy(msg, client_pipe_path, FIFO_NAME_SIZE); // No return values are reserved to indicate an error according to POSIX man page
    
    ssize_t written;
    size_t total_written = 0, to_write = 1 + FIFO_NAME_SIZE;
    
    while ((written = write(write_fd, msg + total_written, to_write) ) {

    } 
    
    return -1;
}

int tfs_unmount() {
    /* TODO: Implement this */
    return -1;
}

int tfs_open(char const *name, int flags) {
    /* TODO: Implement this */
    return -1;
}

int tfs_close(int fhandle) {
    /* TODO: Implement this */
    return -1;
}

ssize_t tfs_write(int fhandle, void const *buffer, size_t len) {
    /* TODO: Implement this */
    return -1;
}

ssize_t tfs_read(int fhandle, void *buffer, size_t len) {
    /* TODO: Implement this */
    return -1;
}

int tfs_shutdown_after_all_closed() {
    /* TODO: Implement this */
    return -1;
}
