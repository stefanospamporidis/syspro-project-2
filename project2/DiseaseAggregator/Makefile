OBJS = diseaseAggregator.o List.o 
SOURCE = diseaseAggregator.cpp List.cpp 
HEADER = diseaseAggregator.hpp List.hpp 
OUT = diseaseAggregator
CC = g++
FLAGS = -g -c 


all: $(OBJS)
	$(CC) -g $(OBJS) -o $(OUT)

diseaseAggregator.o: diseaseAggregator.cpp
	$(CC) $(FLAGS) diseaseAggregator.cpp

List.o: List.cpp
	$(CC) $(FLAGS) List.cpp

clean:
	rm -f $(OBJS) $(OUT)