#include "tecnicofs_client_api.h"

#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

int write_fd; // fd of server's FIFO, open for writes only
int read_fd; // fd of this client's FIFO, open for reads only
int session_id;
char pipe_path[FIFO_NAME_SIZE];

int tfs_mount(char const *client_pipe_path, char const *server_pipe_path) {
    if (mkfifo(client_pipe_path, 0777) != 0) 
        return -1;

    if ((write_fd = open(server_pipe_path, O_WRONLY)) != 0) {
        unlink(client_pipe_path);
        return -1;
    }

    char msg[1 + FIFO_NAME_SIZE];
    char *next_byte = msg;
    msg[0] = TFS_OP_CODE_MOUNT;
    strncpy(msg+1, client_pipe_path, FIFO_NAME_SIZE); // No return values are reserved to indicate an error according to POSIX man page
    
    ssize_t written;
    size_t to_write = 1 + FIFO_NAME_SIZE;
    
    while ((written = write(write_fd, next_byte, to_write)) > 0 ) {
        to_write -= (size_t) written;
        if (to_write == 0)
            break;
        next_byte += (size_t) written;
    } 
   
    if (to_write != 0) {
        unlink(client_pipe_path);
        return -1;
    }

    if ((read_fd = open(client_pipe_path, O_RDONLY)) != 0) {
        close(write_fd);
        unlink(client_pipe_path);
        return -1;
    }

    if (read(read_fd, &session_id, sizeof(int)) != sizeof(int) || session_id < 0) {
        close(write_fd);
        close(read_fd);
        unlink(client_pipe_path);
        return -1;
    }
    
    return 0;
}

int tfs_unmount() {
    char opcode = TFS_OP_CODE_UNMOUNT;
    if (write(write_fd, &opcode, sizeof(opcode)) != sizeof(opcode) || write(write_fd, &session_id, sizeof(session_id)) != sizeof(session_id)) {
        close(write_fd);
        close(read_fd);
        unlink(pipe_path);
        return -1;
    } 
    int ret_code;
    if (read(read_fd, &ret_code, sizeof(int)) != sizeof(int)) {
        close(write_fd);
        close(read_fd);
        unlink(pipe_path);
        return -1;
    }
    if (close(write_fd) == -1) 
        ret_code = -1;
    if (close(read_fd) == -1)
        ret_code = -1;
    if (unlink(pipe_path) == -1)
        ret_code = -1;
    return -1;
}

int tfs_open(char const *name, int flags) {
    char opcode = TFS_OP_CODE_OPEN;
    char file_name[MAX_FILE_NAME];
    strncpy(file_name, name, MAX_FILE_NAME); // assuming that name is a string
    if (write(write_fd, &opcode, sizeof(opcode)) != sizeof(opcode) || write(write_fd, &session_id, sizeof(session_id)) != sizeof(session_id) || write(write_fd, file_name, MAX_FILE_NAME) != MAX_FILE_NAME || write(write_fd, &flags, sizeof(flags)) != sizeof(flags)) {
        close(write_fd);
        close(read_fd);
        unlink(pipe_path);
        return -1;
    } 
    int ret_code;
    if (read(read_fd, &ret_code, sizeof(ret_code)) != sizeof(ret_code)) {
        close(write_fd);
        close(read_fd);
        unlink(pipe_path);
        return -1;
    }
    return ret_code;
}

int tfs_close(int fhandle) {
    char opcode = TFS_OP_CODE_CLOSE;
    if (write(write_fd, &opcode, 1) != 1 || write(write_fd, &session_id, sizeof(session_id)) != sizeof(session_id) || write(write_fd, &fhandle, sizeof(fhandle)) != sizeof(fhandle)) {
        close(write_fd);
        close(read_fd);
        unlink(pipe_path);
        return -1;
    } 
    int ret_code;
    if (read(read_fd, &ret_code, sizeof(ret_code)) != sizeof(ret_code)) {
        close(write_fd);
        close(read_fd);
        unlink(pipe_path);
        return -1;
    }
    return ret_code;
}

ssize_t tfs_write(int fhandle, void const *buffer, size_t len) {
    char opcode = TFS_OP_CODE_WRITE;
    if (write(write_fd, &opcode, 1) != 1 || write(write_fd, &session_id, sizeof(session_id)) != sizeof(session_id) || write(write_fd, &fhandle, sizeof(fhandle)) != sizeof(fhandle) || write(write_fd, &len, sizeof(len)) != sizeof(len)) {
        close(write_fd);
        close(read_fd);
        unlink(pipe_path);
        return -1;
    } 

    ssize_t written;

    while ((written = write(write_fd, buffer, len)) > 0) {
        len -= (size_t) written;
        buffer += (size_t) buffer;
        if (len == 0)
            break;
    }

    if (len != 0) {
        close(write_fd);
        close(read_fd);
        unlink(pipe_path);
        return -1;
    }

    int ret_code;
    if (read(read_fd, &ret_code, sizeof(ret_code)) != sizeof(ret_code)) {
        close(write_fd);
        close(read_fd);
        unlink(pipe_path);
        return -1;
    }
    return ret_code;
}

ssize_t tfs_read(int fhandle, void *buffer, size_t len) {
    char opcode = TFS_OP_CODE_READ;
    if (write(write_fd, &opcode, 1) != 1 || write(write_fd, &session_id, sizeof(session_id)) != sizeof(session_id) || write(write_fd, &fhandle, sizeof(fhandle)) != sizeof(fhandle) || write(write_fd, &len, sizeof(len)) != sizeof(len)) {
        close(write_fd);
        close(read_fd);
        unlink(pipe_path);
        return -1;
    } 

    int num_bytes;
    if (read(read_fd, &num_bytes, sizeof(num_bytes)) != sizeof(num_bytes)) {
        close(write_fd);
        close(read_fd);
        unlink(pipe_path);
        return -1;
    }

    if (num_bytes == -1)
        return -1;
   
    if (num_bytes == 0)
        return 0;
    len = (size_t) num_bytes; // weird cast 
    ssize_t bytes_read;
    while ((bytes_read = read(read_fd, buffer, len)) > 0) {
        len -= (size_t) bytes_read;
        buffer += (size_t) bytes_read;
    } 
    if (len != 0) { // there was an error in read: we might have read some bytes but the server told us there were more: it's an error in the syscall most likely
        close(write_fd);
        close(read_fd);
        unlink(pipe_path);
        return -1;
    }

    return (ssize_t) num_bytes;
    
}

int tfs_shutdown_after_all_closed() {
    char opcode = TFS_OP_CODE_SHUTDOWN_AFTER_ALL_CLOSED;
    if (write(write_fd, &opcode, 1) != 1 || write(write_fd, &session_id, sizeof(session_id)) != sizeof(session_id)) {
        close(write_fd);
        close(read_fd);
        unlink(pipe_path);
        return -1;
    } 
    int ret_code;
    if (read(read_fd, &ret_code, sizeof(ret_code)) != sizeof(ret_code)) {
        close(write_fd);
        close(read_fd);
        unlink(pipe_path);
        return -1;
    }
    return ret_code;
}
