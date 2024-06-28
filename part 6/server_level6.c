#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdbool.h>
#include <string.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <poll.h>
#include <pthread.h>
#include <errno.h>
#include <time.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/wait.h>

#define PORT 8080
#define MAX_EVENTS 50
#define BUFFER_SIZE 1024
#define ll long long

// Typedef for the handler function
typedef void (*handler_t)(int fd);

// structure for each event source
typedef struct {
    int fd;
    handler_t handler;
    pthread_mutex_t fd_lock; // mutex to synchronize access to fd
} event_source_t;

// structure for the Proactor pattern
typedef struct {
    struct pollfd fds[MAX_EVENTS];
    event_source_t sources[MAX_EVENTS];
    int count;
    pthread_mutex_t lock;
} proactor_t;

// structure for shared data
typedef struct {
    pthread_mutex_t lock;
    ll highest_prime;
    int requestCounter;
} shared_data_t;

shared_data_t *shared_data;
FILE *logFile;

// function declarations
void client_handler(int fd);

// function to calculate (base^exp) % mod
ll powerMod(ll base, ll exp, ll mod) {
    ll result = 1;
    base = base % mod;
    while (exp > 0) {
        if (exp % 2 == 1) // If exp is odd
            result = (result * base) % mod;
        exp = exp >> 1; // Divide exp by 2
        base = (base * base) % mod;
    }
    return result;
}

// function for the Miller-Rabin primality test
bool millerTest(ll d, ll n) {
    ll a = 2 + rand() % (n - 4); // choose a random number in range [2, n-2]
    ll x = powerMod(a, d, n);    // compute (a^d) % n

    if (x == 1 || x == n - 1)
        return true;

    // keep squaring x and reduce d by half until d equals n-1
    while (d != n - 1) {
        x = (x * x) % n;
        d *= 2;

        if (x == 1)
            return false;
        if (x == n - 1)
            return true;
    }
    return false;
}

// function to check primality using Miller-Rabin method
bool isPrime(ll n, int k) {
    if (n <= 1 || n == 4)
        return false;
    if (n <= 3)
        return true;

    ll d = n - 1;
    while (d % 2 == 0)
        d /= 2;

    for (int i = 0; i < k; i++)
        if (!millerTest(d, n))
            return false;

    return true;
}

// initialize the Proactor structure
proactor_t* proactor_init() {
    proactor_t* proactor = malloc(sizeof(proactor_t));
    if (!proactor) return NULL;
    memset(proactor, 0, sizeof(proactor_t));
    pthread_mutex_init(&proactor->lock, NULL);
    return proactor;
}

// wrapper function for event handling
void* event_handler_wrapper(void* arg) {
    event_source_t* source = (event_source_t*)arg;
    pthread_mutex_lock(&source->fd_lock);
    if (source->fd != -1) {
        source->handler(source->fd);
        close(source->fd); // Close fd and mark as closed
        source->fd = -1;
    }
    pthread_mutex_unlock(&source->fd_lock);
    return NULL;
}

// add a file descriptor to the Proactor
void proactor_add_fd(proactor_t* proactor, int fd, handler_t handler) {
    pthread_mutex_lock(&proactor->lock);
    if (proactor->count < MAX_EVENTS) {
        proactor->fds[proactor->count].fd = fd;
        proactor->fds[proactor->count].events = POLLIN;
        proactor->sources[proactor->count].fd = fd;
        proactor->sources[proactor->count].handler = handler;
        pthread_mutex_init(&proactor->sources[proactor->count].fd_lock, NULL);
        proactor->count++;
    }
    pthread_mutex_unlock(&proactor->lock);
}

// run the Proactor
void proactor_run(proactor_t* proactor) {
    while (1) {
        int ret = poll(proactor->fds, proactor->count, -1);
        if (ret > 0) {
            for (int i = 0; i < proactor->count; i++) {
                if (proactor->fds[i].revents & POLLIN) {
                    if (proactor->fds[i].fd == proactor->sources[0].fd) {  // listening socket
                        struct sockaddr_in client_addr;
                        socklen_t client_len = sizeof(client_addr);
                        int client_fd = accept(proactor->fds[i].fd, (struct sockaddr *)&client_addr, &client_len);
                        if (client_fd > 0) {
                            proactor_add_fd(proactor, client_fd, proactor->sources[0].handler);
                        }
                    } else {
                        pid_t pid = fork();
                        if (pid == 0) { // child process
                            close(proactor->fds[0].fd); // close listening socket in child
                            client_handler(proactor->sources[i].fd);
                            exit(0);
                        } else if (pid > 0) { // parent process
                            close(proactor->sources[i].fd); // close client socket in parent
                        } else {
                            perror("fork failed");
                        }
                    }
                }
            }
        }
    }
}

// client handler function
void client_handler(int fd) {
    char buffer[BUFFER_SIZE];
    int valread;
    while ((valread = read(fd, buffer, BUFFER_SIZE - 1)) > 0) {
        buffer[valread] = '\0';
        long long num = atoll(buffer);
        srand(time(0)); // initialize random number generator
        int prime = isPrime(num, 5);

        pthread_mutex_lock(&shared_data->lock);
        shared_data->requestCounter++;
        if (prime) {
            if (num > shared_data->highest_prime) shared_data->highest_prime = num;
            fprintf(logFile, "Request #%d: %lld is prime. Highest prime: %lld\n", shared_data->requestCounter, num, shared_data->highest_prime);
        } else {
            fprintf(logFile, "Request #%d: %lld is not prime.\n", shared_data->requestCounter, num);
        }
        fflush(logFile);

        sprintf(buffer, "Request #%d: %lld is %sprime. Highest prime so far: %lld\n", shared_data->requestCounter, num, prime ? "" : "not ", shared_data->highest_prime);
        send(fd, buffer, strlen(buffer), 0);
        pthread_mutex_unlock(&shared_data->lock);
    }

    if (valread <= 0) {
        printf("Client disconnected: socket fd %d\n", fd);
        close(fd);
    }
}

// initialize shared memory
void init_shared_memory() {
    int fd = shm_open("/shared_memory", O_CREAT | O_RDWR, 0666);
    ftruncate(fd, sizeof(shared_data_t));
    shared_data = mmap(0, sizeof(shared_data_t), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);

    pthread_mutexattr_t mutex_attr;
    pthread_mutexattr_init(&mutex_attr);
    pthread_mutexattr_setpshared(&mutex_attr, PTHREAD_PROCESS_SHARED);
    pthread_mutex_init(&shared_data->lock, &mutex_attr);

    shared_data->highest_prime = 0;
    shared_data->requestCounter = 0;
}

int main() {
    // create socket
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd == 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    // set the socket options
    int opt = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }

    // bind socket to the port
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(PORT);

    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    // listen for incoming connections
    if (listen(listen_fd, 10) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    // open log file
    logFile = fopen("server_log.txt", "w");
    if (!logFile) {
        perror("Failed to open log file");
        exit(EXIT_FAILURE);
    }

    // initialize shared memory
    init_shared_memory();

    // initialize Proactor
    proactor_t* proactor = proactor_init();
    proactor_add_fd(proactor, listen_fd, client_handler);
    proactor_run(proactor);

    // cleanup
    fclose(logFile);
    pthread_mutex_destroy(&shared_data->lock); // destroy the lock
    shm_unlink("/shared_memory"); // unlink the shared memory
    return 0;
}
