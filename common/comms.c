#include <errno.h>
#include <unistd.h>

int write_all(int fd, const void *buffer, size_t len) {
  ssize_t written;
  while (len) {
    written = write(fd, buffer, len);
    if (written == -1 && errno == EINTR)
      continue;
    // according to man(2) write, a write to a special file such as a FIFO that
    // returns 0 has unspecificed results with regards to errors. According to
    // man(3) write, "A write request for {PIPE_BUF} or fewer bytes shall have
    // the following effect: if there is sufficient space available in the pipe,
    // write() shall transfer all the data and return the number of bytes
    // requested." this functions is for communication using pipes and shall be
    // called with PIPE_BUF or less bytes. As such, I consider returning 0 an
    // error. Note that accoridng to the POSIX specification we wouldn't even
    // need the loop for len <= PIPE_BUF
    if (written <= 0)
      return -1;
    len -= (size_t)written;
    if (len == 0)
      return 0;
    buffer += (size_t)written;
  }
  return 0;
}

int read_all(int fd, void *buffer, size_t len) {
  ssize_t num_bytes_read;
  while (len) {
    num_bytes_read = read(fd, buffer, len);
    if (num_bytes_read == -1 && errno == EINTR)
      continue;
    if (num_bytes_read <= 0)
      return -1;
    len -= (size_t)num_bytes_read;
    if (len == 0)
      return 0;
    buffer += (size_t)num_bytes_read;
  }
  return 0;
}
