#ifndef COMMON_H
#define COMMON_H

#define FIFO_NAME_SIZE (40)
#define MAX_FILE_NAME (40)

/* tfs_open flags */
enum {
  TFS_O_CREAT = 0b001,
  TFS_O_TRUNC = 0b010,
  TFS_O_APPEND = 0b100,
};

/* operation codes (for client-server requests) */
enum {
  TFS_OP_CODE_MOUNT = 1,
  TFS_OP_CODE_UNMOUNT = 2,
  TFS_OP_CODE_OPEN = 3,
  TFS_OP_CODE_CLOSE = 4,
  TFS_OP_CODE_WRITE = 5,
  TFS_OP_CODE_READ = 6,
  TFS_OP_CODE_SHUTDOWN_AFTER_ALL_CLOSED = 7
};

#endif /* COMMON_H */
