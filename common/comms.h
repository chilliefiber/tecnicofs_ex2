#ifndef COMMS_H
#define COMMS_H

int write_all(int fd, const void *buffer, size_t len);
int read_all(int fd, void *buffer, size_t len);

#endif
