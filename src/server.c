#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#define CHUNK_SIZE 1024
#define RECEIVE_TIMEOUT_SECONDS 2

static int parse_port(const char *value, uint16_t *port)
{
    char *end;
    unsigned long parsed = strtoul(value, &end, 10);

    if (*value == '\0' || *end != '\0' || parsed > UINT16_MAX || parsed == 0) {
        return -1;
    }

    *port = (uint16_t)parsed;
    return 0;
}

int main(int argc, char *argv[])
{
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <port> <output-file>\n", argv[0]);
        return EXIT_FAILURE;
    }

    uint16_t port;
    if (parse_port(argv[1], &port) != 0) {
        fprintf(stderr, "Invalid port: %s\n", argv[1]);
        return EXIT_FAILURE;
    }

    FILE *output = fopen(argv[2], "wb");
    if (output == NULL) {
        perror("fopen");
        return EXIT_FAILURE;
    }

    int socket_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (socket_fd < 0) {
        perror("socket");
        fclose(output);
        return EXIT_FAILURE;
    }

    struct timeval timeout = {
        .tv_sec = RECEIVE_TIMEOUT_SECONDS,
        .tv_usec = 0
    };
    if (setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                   sizeof(timeout)) < 0) {
        perror("setsockopt");
        close(socket_fd);
        fclose(output);
        return EXIT_FAILURE;
    }

    struct sockaddr_in server_address;
    memset(&server_address, 0, sizeof(server_address));
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = htonl(INADDR_ANY);
    server_address.sin_port = htons(port);

    if (bind(socket_fd, (struct sockaddr *)&server_address,
             sizeof(server_address)) < 0) {
        perror("bind");
        close(socket_fd);
        fclose(output);
        return EXIT_FAILURE;
    }

    printf("Listening on UDP port %u...\n", port);

    unsigned char buffer[CHUNK_SIZE];
    struct sockaddr_in client_address;
    socklen_t client_address_length = sizeof(client_address);
    int received_data = 0;
    size_t total_bytes = 0;

    for (;;) {
        ssize_t received = recvfrom(socket_fd, buffer, sizeof(buffer), 0,
                                    (struct sockaddr *)&client_address,
                                    &client_address_length);
        if (received < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (received_data) {
                    break;
                }
                fprintf(stderr, "Timed out waiting for transfer data.\n");
                close(socket_fd);
                fclose(output);
                return EXIT_FAILURE;
            }
            if (errno == EINTR) {
                continue;
            }
            perror("recvfrom");
            close(socket_fd);
            fclose(output);
            return EXIT_FAILURE;
        }

        if (fwrite(buffer, 1, (size_t)received, output) != (size_t)received) {
            perror("fwrite");
            close(socket_fd);
            fclose(output);
            return EXIT_FAILURE;
        }

        received_data = 1;
        total_bytes += (size_t)received;
        printf("Received chunk: %zd bytes (total: %zu)\n",
               received, total_bytes);
    }

    if (fclose(output) != 0) {
        perror("fclose");
        close(socket_fd);
        return EXIT_FAILURE;
    }
    close(socket_fd);

    printf("Transfer complete. Received %zu bytes.\n", total_bytes);
    return EXIT_SUCCESS;
}