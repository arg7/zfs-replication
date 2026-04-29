#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <string.h>
#include <errno.h>
#include <signal.h>

#define BUF_SIZE 1024 * 1024
#define WRITE_CHUNK 1024

char g_cnt_file[1024];
unsigned long long g_total_bytes = 0;

void write_progress() {
    FILE *fp = fopen(g_cnt_file, "w");
    if (fp) {
        fprintf(fp, "%llu\n", g_total_bytes);
        fclose(fp);
    }
}

void handle_signal(int sig) {
    write_progress();
    exit(0);
}

static int parse_rate(const char *s) {
    char *end;
    long val = strtol(s, &end, 10);
    if (val <= 0 || *end == '\0') return (int)val;
    switch (*end) {
        case 'k': case 'K': val *= 1024; break;
        case 'm': case 'M': val *= 1024 * 1024; break;
        case 'g': case 'G': val *= 1024 * 1024 * 1024; break;
        default: return 0;
    }
    return (int)val;
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <lock_file> <interval_sec> [timeout_sec] [write_rate]\n", argv[0]);
        fprintf(stderr, "  write_rate: bytes/sec (supports k/M/G suffix), e.g. 16k\n");
        return 1;
    }

    const char *lock_base = argv[1];
    int interval = atoi(argv[2]);
    int timeout_sec = (argc >= 4) ? atoi(argv[3]) : 0;
    int write_rate = (argc >= 5) ? parse_rate(argv[4]) : 0;
    snprintf(g_cnt_file, sizeof(g_cnt_file), "%s.cnt", lock_base);

    signal(SIGTERM, handle_signal);
    signal(SIGINT, handle_signal);

    unsigned char *buffer = malloc(BUF_SIZE);
    if (!buffer) return 1;

    time_t start_time = time(NULL);
    time_t last_update = 0;
    ssize_t bytes_read;

    // For write throttling: track bytes written in the current second
    int chunk_size = (write_rate > 0 && write_rate < WRITE_CHUNK) ? write_rate : WRITE_CHUNK;
    struct timespec chunk_delay = {0, 0};
    if (write_rate > 0) {
        double delay_ns = (double)chunk_size / write_rate * 1000000000.0;
        chunk_delay.tv_sec  = (time_t)(delay_ns / 1000000000.0);
        chunk_delay.tv_nsec = (long)(delay_ns - chunk_delay.tv_sec * 1000000000.0);
    }

    // Initial write
    write_progress();

    while ((bytes_read = read(STDIN_FILENO, buffer, BUF_SIZE)) > 0) {
        if (timeout_sec > 0 && (time(NULL) - start_time >= timeout_sec)) {
            fprintf(stderr, "iomon: timeout after %ds\n", timeout_sec);
            write_progress();
            free(buffer);
            return 143;
        }

        ssize_t bytes_written = 0;
        while (bytes_written < bytes_read) {
            if (timeout_sec > 0 && (time(NULL) - start_time >= timeout_sec)) {
                fprintf(stderr, "iomon: timeout after %ds\n", timeout_sec);
                write_progress();
                free(buffer);
                return 143;
            }

            ssize_t to_write = bytes_read - bytes_written;
            if (write_rate > 0 && to_write > chunk_size)
                to_write = chunk_size;

            ssize_t res = write(STDOUT_FILENO, buffer + bytes_written, to_write);
            if (res < 0) {
                if (errno == EINTR) continue;
                free(buffer);
                return 1;
            }
            bytes_written += res;

            if (write_rate > 0 && chunk_delay.tv_nsec > 0)
                nanosleep(&chunk_delay, NULL);
        }

        g_total_bytes += bytes_read;
        time_t now = time(NULL);

        if (now - last_update >= interval) {
            write_progress();
            last_update = now;
        }
    }

    write_progress();
    free(buffer);
    return 0;
}
