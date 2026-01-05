// split_buffer.c
// gcc -pthread -o split_buffer split_buffer.c

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>



typedef struct {
    const char *start;   // pointer to beginning of the slice
    size_t length;       // number of bytes to process
    int id;              // worker id
} WorkerArgs;

void *worker(void *arg) {
    WorkerArgs *wa = (WorkerArgs *)arg;
    size_t* wc = malloc(sizeof(size_t));
    unsigned char is_word = 0;

    *wc = 0;
    for(size_t o=0; o < wa->length; o++) {
        if(wa->start[o] == 0x20 || !(wa->start[o] & 0xF0)) {
            is_word = 0;
        } else {
            if (!is_word) {
                is_word = 1;
                (*wc)++;
            }
        }
    }

    return wc;
}

int main(int agrc, char* argv[]) {
	const char* tc = getenv("NTHREADS");
	unsigned char N = 16;
	
	if (tc != NULL)
		N = atoi(tc);   // number of threads
	if (!N)
		N = 16;

    // Prepare threads
    pthread_t threads[N];
    WorkerArgs args[N];

    const char* buffer0;          // file map
    struct stat st;
    size_t wc = 0;
	unsigned char is_last = 0;
	
	int fd = open(argv[1],O_RDONLY);
	if (fd < 0) {
		perror(argv[1]);
		return(1);
	}

    // try to map the file to buffer0
	if (!fstat(fd,&st)) {
		buffer0 = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
		if (buffer0 == MAP_FAILED) {
			perror("mmap");
			return(1);
		}
	}

    for (int i = 0; i < N; i++) {
        args[i].id = i;

        if (i > 0) {
            args[i].start = args[i-1].start + args[i-1].length;
            args[i].length = args[0].length;
        } else {
            // only when i = 0
            args[0].start = buffer0;
            args[0].length = (st.st_size + N - 1) / N;
        }

        while (args[i].start - buffer0 + args[i].length < st.st_size) {
            // "round" stab length at the next white character
            if(args[i].start[args[i].length-1] == 0x20 || !(args[i].start[args[i].length-1] & 0xF0)) {
                break;
            } else {
                args[i].length++;
            }
        }
		
		// The last chunk could be shorter and less than projected threads
        if (args[i].start - buffer0 + args[i].length >= st.st_size) {
            args[i].length = st.st_size - (args[i].start - buffer0);
			is_last = i+1;
		}

//        printf("Start[%d] start:%p length:%ld first:%c\n",args[i].id,args[i].start,args[i].length,args[i].start[0]);
        //wc += *(size_t*) worker(&args[i]);
        pthread_create(&threads[i], NULL, worker, &args[i]);
		
		if (is_last) break;
    }

    // Wait for all threads
    for (int i = 0; i < is_last; i++) {
        void* retval;
        pthread_join(threads[i], &retval);
        wc += *(size_t*)retval;
        free(retval);
    }

    printf("WC:%ld\n",wc);
    return 0;
}
