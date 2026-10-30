
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netdb.h>
#include <errno.h>

#define BUFFER_SIZE 8192

int main(int argc, char **argv)
{
    if (argc != 2) {
        fprintf(stderr,
            "Usage: %s <port>\n",
            argv[0]);
        return 1;
    }

    struct addrinfo hints, *result, *rp;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    int status = getaddrinfo(
        NULL, argv[1], &hints, &result
    );

    if (status != 0) {
        fprintf(stderr, "getaddrinfo: %s\n",
                gai_strerror(status));
        return 1;
    }

    int listenfd = -1;

    for (rp = result; rp != NULL; rp = rp->ai_next) {

        listenfd = socket(
            rp->ai_family,
            rp->ai_socktype,
            rp->ai_protocol
        );

        if (listenfd == -1)
            continue;

        int option = 1;

        setsockopt(
            listenfd,
            SOL_SOCKET,
            SO_REUSEADDR,
            &option,
            sizeof(option)
        );

        if (bind(listenfd, rp->ai_addr,
                 rp->ai_addrlen) == 0)
            break;

        close(listenfd);
        listenfd = -1;
    }

    freeaddrinfo(result);

    if (listenfd == -1) {
        perror("bind");
        return 1;
    }

    if (listen(listenfd, 5) < 0) {
        perror("listen");
        close(listenfd);
        return 1;
    }

    printf("[SERVER] Listening on port %s\n", argv[1]);

    FILE *log = fopen("received_traffic.log", "a");

    if (!log) {
        perror("log file");
        close(listenfd);
        return 1;
    }

    struct sockaddr_storage client_addr;
    socklen_t addr_len = sizeof(client_addr);

    int connfd = accept(
        listenfd,
        (struct sockaddr *)&client_addr,
        &addr_len
    );

    if (connfd < 0) {
        perror("accept");
        fclose(log);
        close(listenfd);
        return 1;
    }

    printf("[SERVER] Client connected\n");

    char buffer[BUFFER_SIZE];
    char record[BUFFER_SIZE * 2];

    size_t used = 0;
    unsigned long count = 0;

    while (1) {

        ssize_t n = recv(
            connfd,
            buffer,
            sizeof(buffer),
            0
        );

        if (n == 0)
            break;

        if (n < 0) {
            perror("recv");
            break;
        }

        for (ssize_t i = 0; i < n; i++) {

            if (buffer[i] == '\n') {

                record[used] = '\0';

                fprintf(log, "%s\n", record);
                fflush(log);

                count++;

                printf("[SERVER] Received record %lu\n",
                       count);

                used = 0;
            }
            else {

                if (used < sizeof(record) - 1) {
                    record[used++] = buffer[i];
                }
                else {
                    fprintf(stderr,
                        "[SERVER] Record too large; discarding\n");
                    used = 0;
                }
            }
        }
    }

    if (used > 0) {
        record[used] = '\0';
        fprintf(log, "%s\n", record);
        count++;
    }

    printf("[SERVER] Connection closed\n");
    printf("[SERVER] Total records received: %lu\n", count);

    fclose(log);
    close(connfd);
    close(listenfd);

    return 0;
}
