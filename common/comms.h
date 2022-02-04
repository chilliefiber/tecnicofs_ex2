#ifndef COMMS_H
#define COMMS_H

#include <stddef.h> // after make fmt it complained it didn't know size_t

// Note that these functions are very rudimentary: when writing to pipes
// you need to make sure the len is PIPE_BUF or smaller

int write_all(int fd, const void *buffer, size_t len);
int read_all(int fd, void *buffer, size_t len);

#endif
