#ifndef diseaseAggregator_hpp
#define diseaseAggregator_hpp

using namespace  std;

#include <stdio.h>
#include <string>
#include <iostream>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <signal.h>
#include <csignal>
#include <fcntl.h>
#include <poll.h>
#include <stdlib.h>
#include <dirent.h>

#include "List.hpp"


typedef struct Parameters
{
	char input_dir[1024];
	int numWorkers;
	int bufferSize;
	int number_of_countries_per_worker; //ftiaxnoume mia parapanw 8esi gia ti periptwsi opou oi xwres px einai 11 kai oi workers 3

}Parameters;

typedef struct{
	char fifo_name[100];
	int filedesc;
	int already_read;
}Fifo;

typedef struct{
	char **countries_array;
	int pid;
	int countries_num;
}Workers_info;

int isNumber(char* number);  
inline bool file_exists (string& name);
void getArgs(int argc,char **argv);
void printArgs(void);
char* readCommands();
void Put_Countries_to_List(List &list,char *input_dir);
void Create_Fifos(Fifo *fifo_read,Fifo *fifo_write);
void Create_Workers_info(Workers_info *wi,List &list);
void Send_Message(int fd,char *message); // stelnei ena minuma sto receiver
void Receive_Message(int fd);
void Send_Countries_to_Workers(Fifo *fifos,Workers_info *workers);
void Send_Commands_to_Workers(Fifo *fifos,char * command);
void Initialize_already_read(Fifo *fifos);
bool Exist_not_sent_Workers(Fifo *fifos);
void Receive_Info_from_Workers(Fifo *fifos);
void Open_Fifos_Read(Fifo *fifos_read);
void Open_Fifos_Write(Fifo *fifos_write);
void Termination_Statistics(Fifo *fifos_read,Fifo *fifos_write,Workers_info *wi);



#endif