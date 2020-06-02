using namespace  std;

#include <stdio.h>
#include <string>
#include <string.h>
#include <iostream>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <signal.h>
#include <fcntl.h>
#include <poll.h>
#include <stdlib.h>
#include <dirent.h>

typedef struct Parameters
{
	char fifo_read[100];
	char fifo_write[100];
	int buffer_size;
	int fd_read;
	int fd_write;

}Parameters;

Parameters params;

void Send_Message(int fd,char *message){
	int size = strlen(message); //size tou message pou 9a steilei o sender
	char temp[10]; //ftiaxnw ena temp
	sprintf(temp,"%d$",size); // to temp krataei to mege8os tou message px 1024$
	write(fd,temp,10); //kanw write sto fifo to 1024$
	write(fd,message,strlen(message)); //kanw write to message
}

char* Receive_Message(int fd){
	char *input_message; // to message
	char temp[10]; //to mege8os tou message
	char *token; //8a apomonwsei ton ari8mo apo to dollario
	read(fd,temp,10); //diavazw to temp pou einai to mege8os tou message
	token = strtok(temp,"$"); //aponwnw ton ari8mo gia na 3erw posa bytes 8a diavasw
	int size = atoi(token); //te metatrepw se ari8mo
	input_message = (char*) malloc(size * sizeof(char) + 1); //o pinakas pou 8a valw mesa to message

	int counter = 0; //counter pou au3anetai mexri na ginei megalutero iso tou size
	int n = 0; //krataei posous xaraktites diavasame
	char *packet_ptr = input_message; //deixnei se poio simeio 8a gra4oume sto message
	while (counter < size){ //ekteleitai mexri na ginei megalutero iso tou size tou message
		n = read(fd,packet_ptr,params.buffer_size); //oi xaraktires pou diavase
		packet_ptr += n; //proxwrame n xaraktires
		counter += n; //au3anoume to counter n xaraktires
	}
	input_message[size] = 0; //kanoume to teleutaio xaraktira tou message 0

}


//prwta dinoume argv1 to fifo read, argv2 to fifo write kai to argv3 8a einai to buffer size
int main(int argc,char **argv){
	
	params.buffer_size = atoi(argv[3]);

	strcpy(params.fifo_read,argv[1]);
	strcpy(params.fifo_write,argv[2]);

	//cout<<"eimai o worker"<<endl;

	params.fd_read = open(params.fifo_read,O_RDONLY);
	params.fd_write = open(params.fifo_write,O_WRONLY);

	char *message = Receive_Message(params.fd_read);

	FILE *f = fopen("test","w");
	fprintf(f, "%s\n", message);
	fclose(f);
	//cout << "eimai o worker\n " << message << endl;

}