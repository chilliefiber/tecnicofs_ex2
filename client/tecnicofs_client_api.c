#include "tecnicofs_client_api.h"
#include "common/comms.h"

#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <limits.h>

int write_fd; // fd of server's FIFO, open for writes only
int read_fd; // fd of this client's FIFO, open for reads only
int session_id;
char pipe_path[FIFO_NAME_SIZE];


int tfs_mount(char const *client_pipe_path, char const *server_pipe_path) {
    if (unlink(client_pipe_path) != 0 && errno != ENOENT)
        return -1;
    if (mkfifo(client_pipe_path, 0777) != 0) 
        return -1;

    if ((write_fd = open(server_pipe_path, O_WRONLY)) == -1) {
        unlink(client_pipe_path);
        return -1;
    }
    char msg[PIPE_BUF];
    if (1 + FIFO_NAME_SIZE > PIPE_BUF) // 1 + FIFO_NAME_SIZE is the actual size of the message we are going to send
        return -1;
    msg[0] = TFS_OP_CODE_MOUNT;
    strncpy(msg+1, client_pipe_path, FIFO_NAME_SIZE); // No return values are reserved to indicate an error according to POSIX man page
   
    if (write_all(write_fd, msg, 1 + FIFO_NAME_SIZE) != 0) {
        close(write_fd);
        unlink(client_pipe_path);
        return -1;
    }

    if ((read_fd = open(client_pipe_path, O_RDONLY)) == -1) {
        close(write_fd);
        unlink(client_pipe_path);
        return -1;
    }

    if (read_all(read_fd, &session_id, sizeof(int)) != 0 || session_id < 0) {
        close(write_fd);
        close(read_fd);
        unlink(client_pipe_path);
        return -1;
    }
    memcpy(pipe_path, client_pipe_path, FIFO_NAME_SIZE); 
    return 0;
}

int tfs_unmount() {
    char buffer[PIPE_BUF];
    size_t len = 1 + sizeof(session_id);
    if (len > PIPE_BUF)
        return -1;
    buffer[0] = TFS_OP_CODE_UNMOUNT;
    memcpy(buffer + 1, &session_id, sizeof(session_id));
    if (write_all(write_fd, buffer, len) != 0) {
        close(write_fd);
        close(read_fd);
        unlink(pipe_path);
        return -1;
    } 
    int ret_code;
    if (read_all(read_fd, &ret_code, sizeof(int)) != 0) {
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
    return ret_code;
}

int tfs_open(char const *name, int flags) {
    char buffer[PIPE_BUF];
    size_t len = 1 + sizeof(session_id) + MAX_FILE_NAME + sizeof(flags);
    if (len > PIPE_BUF)
        return -1;
    buffer[0] = TFS_OP_CODE_OPEN;
    size_t offset = 1;
    memcpy(buffer + offset, &session_id, sizeof(session_id));
    offset += sizeof(session_id);
    strncpy(buffer + offset, name, MAX_FILE_NAME); 
    offset += MAX_FILE_NAME;
    memcpy(buffer + offset, &flags, sizeof(flags));
    if (write_all(write_fd, buffer, len) != 0) {
        close(write_fd);
        close(read_fd);
        unlink(pipe_path);
        return -1;
    } 
    int ret_code;
    if (read_all(read_fd, &ret_code, sizeof(ret_code)) != 0) {
        close(write_fd);
        close(read_fd);
        unlink(pipe_path);
        return -1;
    }
    return ret_code;
}

int tfs_close(int fhandle) {
    char buffer[PIPE_BUF];
    size_t len = 1 + sizeof(session_id) + sizeof(fhandle);
    if (len > PIPE_BUF)
        return -1;
    buffer[0] = TFS_OP_CODE_CLOSE;
    size_t offset = 1;
    memcpy(buffer + offset, &session_id, sizeof(session_id));
    offset += sizeof(session_id);
    memcpy(buffer + offset, &fhandle, sizeof(fhandle));
    if (write_all(write_fd, buffer, len) != 0) {
        close(write_fd);
        close(read_fd);
        unlink(pipe_path);
        return -1;
    } 
    int ret_code;
    if (read_all(read_fd, &ret_code, sizeof(ret_code)) != 0) {
        close(write_fd);
        close(read_fd);
        unlink(pipe_path);
        return -1;
    }
    return ret_code;
}

ssize_t tfs_write(int fhandle, void const *buffer, size_t len) {
    char send_buffer[PIPE_BUF];
    size_t fixed_buf_len = 1 + sizeof(session_id) + sizeof(fhandle) + sizeof(len); 
    if (fixed_buf_len > PIPE_BUF)
        return -1;
    size_t buf_len = fixed_buf_len + len;
    // in this case there are enough bytes to send the constant size part of the message
    // but not enough for all the bytes we want to write: we write only the bytes that fit
    // inside the buffer that can be atomically written into a named pipe
    if (buf_len > PIPE_BUF) {
        len = PIPE_BUF - fixed_buf_len;
        buf_len = PIPE_BUF;
    }
    send_buffer[0] = TFS_OP_CODE_WRITE;
    size_t offset = 1;
    memcpy(send_buffer + offset, &session_id, sizeof(session_id));
    offset += sizeof(session_id);
    memcpy(send_buffer + offset, &fhandle, sizeof(fhandle));
    offset += sizeof(fhandle);
    memcpy(send_buffer + offset, &len, sizeof(len));
    offset += sizeof(len);
    if (len > 0) // there is nothing in the manpage about memcpy with n == 0
        memcpy(send_buffer + offset, buffer, len);
    if (write_all(write_fd, send_buffer, buf_len) != 0) {
        close(write_fd);
        close(read_fd);
        unlink(pipe_path);
        return -1;
    } 
    int ret_code;
    if (read_all(read_fd, &ret_code, sizeof(ret_code)) != 0) {
        close(write_fd);
        close(read_fd);
        unlink(pipe_path);
        return -1;
    }
    return ret_code;
}

ssize_t tfs_read(int fhandle, void *buffer, size_t len) {
    char send_buffer[PIPE_BUF];
    size_t buf_len = 1 + sizeof(session_id) + sizeof(fhandle) + sizeof(len); 
    if (buf_len > PIPE_BUF)
        return -1;
    send_buffer[0] = TFS_OP_CODE_READ;
    size_t offset = 1;
    memcpy(send_buffer + offset, &session_id, sizeof(session_id));
    offset += sizeof(session_id);
    memcpy(send_buffer + offset, &fhandle, sizeof(fhandle));
    offset += sizeof(fhandle);
    memcpy(send_buffer + offset, &len, sizeof(len));
    if (write_all(write_fd, send_buffer, buf_len) != 0) {
        close(write_fd);
        close(read_fd);
        unlink(pipe_path);
        return -1;
    } 
    int ret_code;
    if (read_all(read_fd, &ret_code, sizeof(ret_code)) != 0) { 
        close(write_fd);
        close(read_fd);
        unlink(pipe_path);
        return -1;
    }
    if (ret_code == 0 || ret_code == -1) // if it is 0 no bytes were read but there was no error, if it is -1 the server sent an error value
        return ret_code;

    // now we know ret_code is a positive value containing the number of bytes the server sent for reading
    if (read_all(read_fd, buffer, (size_t) ret_code) != 0) {
        close(write_fd);
        close(read_fd);
        unlink(pipe_path);
        return -1;
    }
    return (ssize_t) ret_code;
}

int tfs_shutdown_after_all_closed() {
    char buffer[PIPE_BUF];      
    size_t len = 1 + sizeof(session_id);
    if (len > PIPE_BUF)
        return -1;
    buffer[0] = TFS_OP_CODE_SHUTDOWN_AFTER_ALL_CLOSED;
    size_t offset = 1;
    memcpy(buffer + offset, &session_id, sizeof(session_id));
    if (write_all(write_fd, buffer, len) != 0) {
        close(write_fd);
        close(read_fd);
        unlink(pipe_path);
        return -1;
    } 
    int ret_code;
    if (read_all(read_fd, &ret_code, sizeof(ret_code)) != 0) {
        close(write_fd);
        close(read_fd);
        unlink(pipe_path);
        return -1;
    }
    return ret_code;
}
