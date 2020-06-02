OBJS = MainUtils.o Database.o Structs.o AVL_Heap.o
SOURCE = MainUtils.cpp Database.cpp Structs.cpp AVL_Heap.cpp
HEADER = MainUtils.hpp Database.hpp Structs.hpp AVL_Heap.hpp
OUT = diseaseMonitor
CC = g++
FLAGS = -g -c 


all: $(OBJS)
	$(CC) -g $(OBJS) -o $(OUT)

MainUtils.o: MainUtils.cpp
	$(CC) $(FLAGS) MainUtils.cpp

Database.o: Database.cpp
	$(CC) $(FLAGS) Database.cpp

Structs.o: Structs.cpp
	$(CC) $(FLAGS) Structs.cpp

AVL_Heap.o: AVL_Heap.cpp
	$(CC) $(FLAGS) AVL_Heap.cpp

clean:
	rm -f $(OBJS) $(OUT)
