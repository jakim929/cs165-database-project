#include <sys/types.h>
#include <sys/mman.h>
#include <stdio.h>
#include <fcntl.h>
#include <stddef.h>
#include <unistd.h>

void* mmap_to_write(char* path, size_t capacity) {
	int rflag = -1;
	int fd = open(path, O_RDWR | O_CREAT, (mode_t)0600);

	if(fd == -1) {
		return NULL;
	}

	rflag = lseek(fd, capacity - 1, SEEK_SET);

	if (rflag == -1) {
		close(fd);
		return NULL;
	}

	rflag = write(fd, "", 1);

	if (rflag == -1) {
		close(fd);
		return NULL;
	}

	void* data = mmap(0, capacity, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
	close(fd);
	return data;
}

void* mmap_to_read(char* path, size_t size) {
	int rflag = -1;
	int fd = open(path, O_RDWR, (mode_t)0600);
    if(fd == -1) {
		return NULL;
	}
	rflag = lseek(fd, size - 1, SEEK_SET);
	if (rflag == -1) {
		close(fd);
		return NULL;
	}
	rflag = write(fd, "", 1);
	if (rflag == -1) {
		close(fd);
		return NULL;
	}

    void* ptr = mmap(0, size, PROT_READ, MAP_SHARED, fd, 0);
	close(fd);
    return ptr;
}

void* resize_mmap_write_capacity(void* data, char* data_path, size_t prev_size, size_t new_size) {
	int rflag = msync(data, prev_size, MS_SYNC);
    if(rflag == -1)
    {
        return NULL;
    }
    rflag = munmap(data, prev_size);
    if(rflag == -1)
    {
        return NULL;
    }

	void* ptr = mmap_to_write(data_path, new_size);
	return ptr;
}
