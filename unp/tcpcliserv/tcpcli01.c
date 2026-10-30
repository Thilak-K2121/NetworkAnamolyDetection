
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
    if (argc != 4) {
        fprintf(stderr,
            "Usage: %s <server-ip> <port> <csv-file>\n",
            argv[0]);
        return 1;
    }

    const char *server_ip = argv[1];
    const char *port = argv[2];
    const char *csv_file = argv[3];

    FILE *fp = fopen(csv_file, "r");

    if (!fp) {
        perror("CSV file");
        return 1;
    }

    struct addrinfo hints, *result, *rp;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    int status = getaddrinfo(
        server_ip, port, &hints, &result
    );

    if (status != 0) {
        fprintf(stderr, "getaddrinfo: %s\n",
                gai_strerror(status));
        fclose(fp);
        return 1;
    }

    int sockfd = -1;

    for (rp = result; rp != NULL; rp = rp->ai_next) {

        sockfd = socket(
            rp->ai_family,
            rp->ai_socktype,
            rp->ai_protocol
        );

        if (sockfd == -1)
            continue;

        if (connect(sockfd, rp->ai_addr,
                    rp->ai_addrlen) == 0)
            break;

        close(sockfd);
        sockfd = -1;
    }

    freeaddrinfo(result);

    if (sockfd == -1) {
        fprintf(stderr, "Could not connect to server\n");
        fclose(fp);
        return 1;
    }

    printf("[CLIENT] Connected to server\n");

    char buffer[BUFFER_SIZE];

    // Skip CSV header.
    if (!fgets(buffer, sizeof(buffer), fp)) {
        fprintf(stderr, "Empty CSV file\n");
        close(sockfd);
        fclose(fp);
        return 1;
    }

    unsigned long count = 0;

    while (fgets(buffer, sizeof(buffer), fp)) {

        size_t length = strlen(buffer);

        // Ensure newline framing.
        if (length == 0 ||
            buffer[length - 1] != '\n') {

            if (length + 1 >= sizeof(buffer)) {
                fprintf(stderr, "CSV row too long\n");
                break;
            }

            buffer[length] = '\n';
            buffer[length + 1] = '\0';
            length++;
        }

        size_t sent = 0;

        while (sent < length) {

            ssize_t n = send(
                sockfd,
                buffer + sent,
                length - sent,
                0
            );

            if (n <= 0) {
                perror("send");
                goto cleanup;
            }

            sent += (size_t)n;
        }

        count++;

        printf("[CLIENT] Sent record %lu\n", count);

        usleep(10000);
    }

    printf("[CLIENT] Transmission completed: %lu rows\n",
           count);

cleanup:

    shutdown(sockfd, SHUT_WR);

    close(sockfd);
    fclose(fp);

    return 0;
}
