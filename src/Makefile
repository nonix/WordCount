CC = gcc
CFLAGS = -O3 -m64 -s
DEBUG_CFLAGS = -g -O0 -m64
LDFLAGS = -pthread

TARGET = wc
SRC = main.c

all: $(TARGET)

$(TARGET): $(SRC)
        $(CC) $(CFLAGS) $(SRC) -o $(TARGET) $(LDFLAGS)

debug: $(SRC)
        $(CC) $(DEBUG_CFLAGS) $(SRC) -o $(TARGET)_debug $(LDFLAGS)

clean:
        rm -f $(TARGET) $(TARGET)_debug
