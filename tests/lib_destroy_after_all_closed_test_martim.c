#include "fs/operations.h"
#include <assert.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>
#define COUNT 2
#define SIZE 512
#define RW_THREAD_COUNT 1
#define THREAD_COUNT (RW_THREAD_COUNT + 2) // one for the destroy, another for the open_fail
/*  Simple test to check whether the implementation of
    tfs_destroy_after_all_closed is correct.
    Note: This test uses TecnicoFS as a library, not
    as a standalone server.
    We recommend trying more elaborate tests of tfs_destroy_after_all_closed.
    Also, we suggest trying out a similar test once the
    client-server version is ready (calling the tfs_shutdown_after_all_closed 
    operation).
*/
void *rw_thread(void *arg) {
    char *path = (char *) arg;
    char input[SIZE]; 
    memset(input, 'A', SIZE);

    char output [SIZE];

    int fd_write = tfs_open(path, TFS_O_CREAT), fd_read = tfs_open(path, TFS_O_CREAT);
    assert(fd_write != -1);
    sleep(3); // this is to make sure that we have already called tfs_destroy_after_closed when we do the reads/writes/final open
    ssize_t write_ret, read_ret;
    for (int i = 0; i < COUNT; i++) {
        write_ret = tfs_write(fd_write, input, SIZE);
        assert(write_ret == SIZE);
    }

    assert(tfs_close(fd_write) != -1);
    assert(fd_read != -1 );
    for (int i = 0; i < COUNT; i++) {
        read_ret = tfs_read(fd_read, output, SIZE);
        assert(read_ret == SIZE);
        assert (memcmp(input, output, SIZE) == 0);
    }

    assert(tfs_close(fd_read) != -1);
    sleep(1);
    fd_write = tfs_open(path, TFS_O_CREAT);
    assert(fd_write == -1);
    return NULL;
}

void *destroy() {
    assert(tfs_destroy_after_all_closed() != -1);
    return NULL;
}

void *fail_to_open_prevent_starvation() {
    assert(tfs_open("/some_path", TFS_O_CREAT) == -1);
    return NULL;
}

int main() {
    assert(tfs_init() != -1);
    char *paths[RW_THREAD_COUNT];
    paths[0] = strdup("/f1");
    pthread_t threads[THREAD_COUNT];
    for (int a = 0; a < RW_THREAD_COUNT; a++) {
        assert(pthread_create(&threads[a], NULL, rw_thread, (void *) paths[a]) == 0);
    }
    sleep(1); // this sleep is to make sure we have opened the files in fn_thread
    assert(pthread_create(&threads[RW_THREAD_COUNT], NULL, destroy, NULL) == 0); 
    assert(pthread_create(&threads[RW_THREAD_COUNT + 1], NULL, fail_to_open_prevent_starvation, NULL) == 0);
    for (int a = 0; a<THREAD_COUNT;a++) {
        assert(pthread_join(threads[a], NULL) == 0);
        if (a < RW_THREAD_COUNT)
            free(paths[a]);
    }
    printf("Successful test.\n");

    return 0;
}
 
