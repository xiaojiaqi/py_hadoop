#include <stdlib.h>
#include <stdio.h>
#include <time.h>

int main(int argc, char* argv[]) {
    srand(time(NULL));
    int linecount = 1000;
    if (argc >= 2) {
        linecount = atoi(argv[1]);
    }

    char* buff = new char[1024 * 1024];

    int len = 0;
    int offset = 0;
    for (int i = 0; i < linecount; ++i) {
        unsigned long long x = (unsigned long long)rand() % 9 + 1;
        *(buff + offset + x) = '\n';
        *(buff + offset + x + 1) = 0;
        for (int j = x - 1; j >= 1; j--) {
            *(buff + offset + j) = rand() % 10 + '0';
        }
        *(buff + offset) = rand() % 9 + 1 + '0';
        offset += x + 1;
        len++;
        if (len >= 1024 * 1024 / 11) {
            printf("%s", buff);
            len = 0;
            offset = 0;
        }
    }
    if (len > 0) {
        printf("%s", buff);
    }
    delete[] buff;
    return 0;
}
