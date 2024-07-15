#include "helper_funcs.h"
#include "connection.h"
#include "debug.h"
#include "request.h"
#include "response.h"
#include "queue.h"

#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>

#include <sys/file.h>
#include <sys/stat.h>

void handle_connection(uintptr_t);
void handle_get(conn_t *);
void handle_put(conn_t *);
void handle_unsupported(conn_t *);

queue_t *requests_queue;

pthread_mutex_t file_mutex = PTHREAD_MUTEX_INITIALIZER;

void *worker_routine(void *args) {
    (void) args;
    while (true) {
        uintptr_t connfd;
        if (queue_pop(requests_queue, (void **) &connfd)) {
            handle_connection(connfd);
            close(connfd);
        }
    }
    return NULL;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        warnx("wrong arguments: %s [-t threads] port_num", argv[0]);
        fprintf(stderr, "usage: %s [-t threads] <port>\n", argv[0]);
        return EXIT_FAILURE;
    }

    size_t port;
    int threads = 4;

    int argInd = 1;
    if (argv[argInd][0] == '-' && argv[argInd][1] == 't') {
        argInd += 2;

        if (argInd >= argc) {
            warnx("missing threads argument for -t option");
            return EXIT_FAILURE;
        }

        threads = atoi(argv[argInd - 1]);
    }

    port = (size_t) strtoull(argv[argInd], NULL, 10);

    signal(SIGPIPE, SIG_IGN);
    Listener_Socket sock;
    listener_init(&sock, port);
    requests_queue = queue_new(threads + 1);

    pthread_t threads_arr[threads];

    for (int i = 0; i < threads; i++) {
        pthread_create(&threads_arr[i], NULL, worker_routine, (void *) (uintptr_t) (i + 1));
    }

    while (1) {
        uintptr_t connfd = listener_accept(&sock);
        queue_push(requests_queue, (void *) connfd);
    }

    for (int i = 0; i < threads; i++) {
        pthread_join(threads_arr[i], NULL);
    }

    queue_delete(&requests_queue);
    pthread_mutex_destroy(&file_mutex);

    return EXIT_SUCCESS;
}

void handle_connection(uintptr_t connfd) {
    conn_t *conn = conn_new(connfd);
    const Response_t *res = conn_parse(conn);

    if (res != NULL) {
        conn_send_response(conn, res);
    } else {
        const Request_t *req = conn_get_request(conn);

        if (req == &REQUEST_GET) {
            handle_get(conn);
        } else if (req == &REQUEST_PUT) {
            handle_put(conn);
        } else {
            handle_unsupported(conn);
        }
    }

    conn_delete(&conn);
    return;
}

void handle_get(conn_t *conn) {

    pthread_mutex_lock(&file_mutex);

    struct stat st;

    char *uri = conn_get_uri(conn);
    char *ID = conn_get_header(conn, "Request-Id");
    if (ID == NULL) {
        ID = "0";
    }

    int getpath = open(uri, O_RDONLY, 0666);
    if (getpath == -1) {
        if (errno == ENOENT) {
            conn_send_response(conn, &RESPONSE_NOT_FOUND);
            fprintf(stderr, "GET,/%s,404,%s\n", uri, ID);
            pthread_mutex_unlock(&file_mutex);
            return;
        } else if (errno == EACCES) {
            conn_send_response(conn, &RESPONSE_FORBIDDEN);
            fprintf(stderr, "GET,/%s,403,%s\n", uri, ID);
            pthread_mutex_unlock(&file_mutex);
            return;
        } else {
            conn_send_response(conn, &RESPONSE_INTERNAL_SERVER_ERROR);
            fprintf(stderr, "GET,/%s,500,%s\n", uri, ID);
            pthread_mutex_unlock(&file_mutex);
            return;
        }
    }

    fstat(getpath, &st);
    int siz = st.st_size;

    if (S_ISDIR(st.st_mode)) {
        conn_send_response(conn, &RESPONSE_FORBIDDEN);
        fprintf(stderr, "GET,/%s,403,%s\n", uri, ID);
        close(getpath);
        pthread_mutex_unlock(&file_mutex);
        return;
    }

    pthread_mutex_unlock(&file_mutex);

    flock(getpath, LOCK_SH);
    conn_send_file(conn, getpath, siz);
    fprintf(stderr, "GET,/%s,200,%s\n", uri, ID);
    flock(getpath, LOCK_UN);

    debug("GET %s", uri);

    close(getpath);

    pthread_mutex_unlock(&file_mutex);

    return;
}

void handle_put(conn_t *conn) {
    // TODO: Implement PUT

    pthread_mutex_lock(&file_mutex);

    char *uri = conn_get_uri(conn);
    char *clength = conn_get_header(conn, "Content-Length");
    char *ID = conn_get_header(conn, "Request-Id");
    if (ID == NULL) {
        ID = "0";
    }

    int putpath;
    int file_status = 0;

    if (clength == NULL) {
        conn_send_response(conn, &RESPONSE_BAD_REQUEST);
        fprintf(stderr, "PUT,/%s,400,%s\n", uri, ID);
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    if (access(uri, F_OK) == -1) {
        putpath = open(uri, O_WRONLY | O_CREAT | O_TRUNC, 0666);
        if (putpath == -1) {
            conn_send_response(conn, &RESPONSE_INTERNAL_SERVER_ERROR);
            fprintf(stderr, "PUT,/%s,500,%s\n", uri, ID);
            pthread_mutex_unlock(&file_mutex);
            return;
        }
        file_status = 1;
    } else {
        putpath = open(uri, O_WRONLY | O_TRUNC, 0666);
        if (putpath == -1) {
            conn_send_response(conn, &RESPONSE_INTERNAL_SERVER_ERROR);
            fprintf(stderr, "PUT,/%s,500,%s\n", uri, ID);
            pthread_mutex_unlock(&file_mutex);
            return;
        }
        file_status = 2;
    }

    pthread_mutex_unlock(&file_mutex);
    flock(putpath, LOCK_EX);
    ftruncate(putpath, 0);
    conn_recv_file(conn, putpath);
    flock(putpath, LOCK_UN);

    if (file_status == 1) {
        conn_send_response(conn, &RESPONSE_CREATED);
        fprintf(stderr, "PUT,/%s,201,%s\n", uri, ID);
    } else {
        conn_send_response(conn, &RESPONSE_OK);
        fprintf(stderr, "PUT,/%s,200,%s\n", uri, ID);
    }

    debug("PUT %s", uri);

    close(putpath);

    // pthread_mutex_unlock(&file_mutex);

    return;
}

void handle_unsupported(conn_t *conn) {
    debug("Unsupported request");
    conn_send_response(conn, &RESPONSE_NOT_IMPLEMENTED);
    return;
}
