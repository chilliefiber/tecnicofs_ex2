#include <unistd.h>

int write_all(int fd, const void *buffer, size_t len) {
    ssize_t written;
    while ((written = write(fd, buffer, len)) > 0 ) {
        len -= (size_t) written;
        if (len == 0)
            return 0;
        buffer += (size_t) written;
    } 
    return -1;
}

int read_all(int fd, void *buffer, size_t len) {
    ssize_t num_bytes_read;
    while ((num_bytes_read = read(fd, buffer, len)) > 0) {
        len -= (size_t) num_bytes_read;
        if (len == 0)
            return 0;
        buffer += (size_t) num_bytes_read;
    }
    return -1;
}
