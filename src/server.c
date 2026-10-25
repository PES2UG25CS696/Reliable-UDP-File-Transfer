#include <arpa/inet.h>
#include <errno.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <sys/time.h>
#include "protocol.h"

#define OUTPUT_FILE "received_file.bin"

int main(void)
{
    int server_socket;
    struct sockaddr_in server_addr;
    struct sockaddr_in client_addr;
    struct sockaddr_in transfer_client={0};
    socklen_t client_len;

    server_socket = socket(AF_INET, SOCK_DGRAM, 0);

    if (server_socket < 0) {
        perror("socket");
        return EXIT_FAILURE;
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(UDP_PORT);
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(server_socket, (struct sockaddr *)&server_addr,
             sizeof(server_addr)) < 0) {
        perror("bind");
        close(server_socket);
        return EXIT_FAILURE;
    }

    printf("Reliable UDP File Transfer Server\n");
    printf("Listening on UDP port %d...\n", UDP_PORT);
    fflush(stdout);

    FILE *file = NULL;
    Packet packet;
    Packet ack;

    uint64_t expected_size = 0;
    uint64_t received_size = 0;
    uint32_t expected_seq = 1;
    uint32_t last_ack = 0;
    int started = 0;
    int success = 0;

    for (;;) {
        memset(&packet, 0, sizeof(packet));
        memset(&client_addr, 0, sizeof(client_addr));
        client_len = sizeof(client_addr);

        ssize_t n = recvfrom(
            server_socket,
            &packet,
            sizeof(packet),
            0,
            (struct sockaddr *)&client_addr,
            &client_len
        );

        if (n < 0) {
            if (errno == EINTR)
                continue;

            perror("recvfrom");
            break;
        }

        if (n < (ssize_t)PACKET_HEADER_SIZE ||
            packet.magic != PACKET_MAGIC) {
            fprintf(stderr, "Ignoring invalid packet header.\n");
            continue;
        }

        if (!started) {
            if (packet.type != PACKET_START ||
                packet.seq != 0 ||
                n != (ssize_t)PACKET_HEADER_SIZE) {
                continue;
            }

            expected_size = packet.total_size;

            file = fopen(OUTPUT_FILE, "wb");

            if (file == NULL) {
                perror("fopen");
                break;
            }

            transfer_client = client_addr;
            started = 1;

            printf("Client connected: %s\n",
                   inet_ntoa(client_addr.sin_addr));
            printf("Expected file size: %" PRIu64 " bytes\n",
                   expected_size);

            memset(&ack, 0, sizeof(ack));
            ack.magic = PACKET_MAGIC;
            ack.type = PACKET_ACK;
            ack.seq = 0;

            ssize_t sent = sendto(
                server_socket,
                &ack,
                PACKET_HEADER_SIZE,
                0,
                (struct sockaddr *)&client_addr,
                client_len
            );

            if (sent != (ssize_t)PACKET_HEADER_SIZE) {
                perror("sendto");
                break;
            }

            if (expected_size == 0) {
                success = 1;
                printf("Empty file received successfully.\n");
                break;
            }

            continue;
        }

        if (client_addr.sin_addr.s_addr !=
                transfer_client.sin_addr.s_addr ||
            client_addr.sin_port != transfer_client.sin_port) {
            continue;
        }

        /*
         * A repeated START packet can occur if the original START ACK
         * was lost. Re-acknowledge it without reopening the output file.
         */
        if (packet.type == PACKET_START &&
            packet.seq == 0 &&
            n == (ssize_t)PACKET_HEADER_SIZE &&
            packet.total_size == expected_size) {

            memset(&ack, 0, sizeof(ack));
            ack.magic = PACKET_MAGIC;
            ack.type = PACKET_ACK;
            ack.seq = 0;

            (void)sendto(
                server_socket, &ack, PACKET_HEADER_SIZE, 0,
                (struct sockaddr *)&client_addr, client_len
            );

            continue;
        }

        if (packet.type != PACKET_DATA ||
            packet.data_len == 0 ||
            packet.data_len > DATA_SIZE ||
            n != (ssize_t)(PACKET_HEADER_SIZE + packet.data_len)) {
            continue;
        }

        if (payload_checksum(packet.data, packet.data_len) !=
            packet.checksum) {
            fprintf(stderr, "Checksum error in packet %" PRIu32 "\n",
                    packet.seq);
            continue;
        }

        memset(&ack, 0, sizeof(ack));
        ack.magic = PACKET_MAGIC;
        ack.type = PACKET_ACK;

        if (packet.seq == expected_seq) {
            if (received_size + packet.data_len > expected_size) {
                fprintf(stderr, "Unexpected file size.\n");
                break;
            }

            size_t written = fwrite(
                packet.data, 1, packet.data_len, file
            );

            if (written != packet.data_len) {
                perror("fwrite");
                break;
            }

            received_size += packet.data_len;
            last_ack = packet.seq;
            ack.seq = packet.seq;
            expected_seq++;

            printf("Received packet %" PRIu32
                   " (%" PRIu32 " bytes; %" PRIu64 "/%" PRIu64 ")\n",
                   packet.seq, packet.data_len,
                   received_size, expected_size);
        } else {
            /*
             * A duplicate packet must not be written twice.
             * Re-acknowledge the last in-order packet.
             */
            ack.seq = last_ack;
        }

        ssize_t sent = sendto(
            server_socket,
            &ack,
            PACKET_HEADER_SIZE,
            0,
            (struct sockaddr *)&client_addr,
            client_len
        );

        if (sent != (ssize_t)PACKET_HEADER_SIZE) {
            perror("sendto");
            break;
        }

        if (received_size == expected_size) {
    if (fflush(file) != 0) {
        perror("fflush");
        success = 0;
        break;
    }

    if (fclose(file) != 0) {
        perror("fclose");
        file = NULL;
        success = 0;
        break;
    }

    file = NULL;
    success = 1;

    printf("File received successfully: %s\n", OUTPUT_FILE);

    /*
     * Keep the server available briefly so it can re-ACK
     * a retransmitted final data packet.
     */
    struct timeval timeout;
    timeout.tv_sec = 3;
    timeout.tv_usec = 0;

    if (setsockopt(server_socket, SOL_SOCKET, SO_RCVTIMEO,
                   &timeout, sizeof(timeout)) < 0) {
        perror("setsockopt");
        break;
    }

    for (;;) {
        Packet final_packet;
        struct sockaddr_in final_client;
        socklen_t final_client_len = sizeof(final_client);

        ssize_t final_n = recvfrom(
            server_socket,
            &final_packet,
            sizeof(final_packet),
            0,
            (struct sockaddr *)&final_client,
            &final_client_len
        );

        if (final_n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;

            if (errno == EINTR)
                continue;

            perror("recvfrom");
            success = 0;
            break;
        }

        if (final_client.sin_addr.s_addr !=
                transfer_client.sin_addr.s_addr ||
            final_client.sin_port != transfer_client.sin_port)
            continue;

        if (final_n < (ssize_t)PACKET_HEADER_SIZE ||
            final_packet.magic != PACKET_MAGIC ||
            final_packet.type != PACKET_DATA ||
            final_packet.seq != last_ack ||
            final_packet.data_len == 0 ||
            final_packet.data_len > DATA_SIZE ||
            final_n != (ssize_t)(PACKET_HEADER_SIZE +
                                 final_packet.data_len))
            continue;

        if (payload_checksum(final_packet.data,
                             final_packet.data_len) !=
            final_packet.checksum)
            continue;

        memset(&ack, 0, sizeof(ack));
        ack.magic = PACKET_MAGIC;
        ack.type = PACKET_ACK;
        ack.seq = last_ack;

        if (sendto(server_socket, &ack, PACKET_HEADER_SIZE, 0,
                   (struct sockaddr *)&final_client,
                   final_client_len) != (ssize_t)PACKET_HEADER_SIZE) {
            perror("sendto final ACK");
            success = 0;
            break;
        }

        printf("Re-acknowledged final packet %" PRIu32 "\n",
               last_ack);
    }

    break;
}
}
   

    if (file != NULL) {
        if (fclose(file) != 0) {
            perror("fclose");
            success = 0;
        }
    }

    close(server_socket);
    printf("Server stopped.\n");

    return success ? EXIT_SUCCESS : EXIT_FAILURE;
}
