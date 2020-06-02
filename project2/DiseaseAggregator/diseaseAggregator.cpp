#include "diseaseAggregator.hpp"

Parameters params;

int isNumber(char* number){ //tsekarw an dinw ari8mo apo to pliktrologio
    int i;
    for (i=0;i<strlen(number);i++){
        if (number[i]<'0' || number[i]>'9') //koitaw an ka8e xaraktiras den einai anamesa stous xaraktires 0 kai 9
            return 0; // an den einai gurnaw 0
    }
    return 1; //alliws gurnaw 1
}   

inline bool file_exists (string& name) { //checkarei an to arxeio pou edwsa uparxei an uparxei gurnaei true
    if (FILE *file = fopen(name.c_str(), "r")) { //alliws false 
        fclose(file);
        return true;
    } else {
        return false;
    }   
} 

void getArgs(int argc,char **argv) {
    
    int w=0 ,b=0 ,input=0;
    string file;
    cout << "You have entered " << argc << " arguments:" << "\n"; 
  
    if (argc != 7){
        cout << "La8os ari8mos orismatwn" << endl;
        exit(1);
    }

    for(int i=1; i < 7; i+=2){
        if (string(argv[i]) == "-w"){  
            if (isNumber(argv[i+1]) == 0){
                cout << "-w must be a number" <<endl;
                exit(1);
            }
            else{
                params.numWorkers = atoi(argv[i+1]);
                w = 1;
            }
        } 
        if (string(argv[i]) == "-b"){ 
            if (isNumber(argv[i+1]) == 0){
                cout << "-b must be a number" <<endl;
                exit(1);
            }
            else{
                params.bufferSize = atoi(argv[i+1]);
                b = 1;
            }
        }  
        if (string(argv[i]) == "-i"){ 
            strcpy(params.input_dir,argv[i+1]);
            file += params.input_dir; //metatropi tou char input_dir se string gia na koita3w an uparxei to file auto
            if (file_exists(file) == false){
               cout << "The file doesn't exist" << endl;
               exit(1);
            }
            input = 1;
        }  
    }
    
    //tsekarw an do8ikan ola ta orismata
    if (w != 1 || b != 1 || input != 1){ //einai oi counter twn orismatwn opote kai an oloi exoun par8ei mia fora tote 8a prepei na einai oloi 1 alliws einai la8os
        cout << "Wrong arguments" << endl << "./diseaseAggregator –w numWorkers -b bufferSize -i input_dir" <<endl;
        exit(1);
    }

}


void printArgs(void) { //kanoume print ta arguments pou dinontai apo ti grammi entolwn
    cout << "Input dir " << params.input_dir << endl ; //perissotero to kanoume gia na epivevaiwsoume oti pernane swsta ta orismata
    cout << "number of workers " << params.numWorkers << endl ;
    cout << "buffer size " << params.bufferSize << endl ;
}

char *readCommands(){

    char *buffer = new char[1024]; //ston buffer 8a mpei i entoli
    int ch; //to xrisimopoiw gia na vrw to telos tis le3is pou einai -1

	memset(buffer,0,1024);
    ch = getchar();
    while ( ch != EOF ){ //oso de pataw enter i diadikasia epanalamvanetai
        switch(ch){
            case '\n':
                //parseCommands(buffer); //dinei stin parseCommands tin entoli pou 8elei na ektelestei
                //cout<< "/";
                return buffer;
				break;
            default:
                strncat(buffer, ((char*) &ch), 1); //pairnei ton epomeno xaraktira , an den ekana cast diladi to ((char*) &ch) tote 8a epairne ton tetarto 
                break;                             //epomeno xaraktika
        }
        ch = getchar();
    }
   
}



void Put_Countries_to_List(List &list,char *input_dir){
	DIR *dir_ptr = opendir(input_dir);
	if (dir_ptr == NULL)
		return;
	struct dirent* dirent_ptr;
	char buff[1024];

	while ((dirent_ptr = readdir(dir_ptr)) != NULL){
		if (dirent_ptr->d_type == DT_DIR && strcmp(dirent_ptr->d_name,".") != 0 && strcmp(dirent_ptr->d_name,"..") != 0){
			sprintf(buff,"%s/%s",input_dir,dirent_ptr->d_name);
			list.AddNode(buff);
		}
	}

	params.number_of_countries_per_worker = (list.counter / params.numWorkers) + 1;
}

void Create_Fifos(Fifo *fifo_read,Fifo *fifo_write){
	struct stat stattemp;
	for (int i =0; i < params.numWorkers; i++){
		sprintf(fifo_read[i].fifo_name,"%s_%d","../Read_Write_Fifos/read",i);
		if (stat(fifo_read[i].fifo_name,&stattemp) == -1){
			if (mkfifo(fifo_read[i].fifo_name,0666) < 0){
				printf("fifo has failed \n");
				exit(1);
			}
		}
	}
	for (int i =0; i < params.numWorkers; i++){
		sprintf(fifo_write[i].fifo_name,"%s_%d","../Read_Write_Fifos/write",i);
		/*if (stat(fifo_write[i].fifo_name,&stattemp) == -1){
			if (mkfifo(fifo_write[i].fifo_name,0666) < 0){
				printf("fifo has failed \n");
				exit(1);
			}
		}*/
	}
}

void Open_Fifos_Read(Fifo *fifos_read){

	for (int i = 0; i < params.numWorkers; i++){
		fifos_read[i].filedesc = open(fifos_read[i].fifo_name, O_RDONLY | O_NONBLOCK);
	}


}

void Open_Fifos_Write(Fifo *fifos_write){

	for (int i = 0; i < params.numWorkers; i++){
		fifos_write[i].filedesc = open(fifos_write[i].fifo_name, O_WRONLY | O_NONBLOCK);
	}	
}

void Create_Workers_info(Workers_info *wi,List &list){
	ListNode *temp = list.head;
	for (int i = 0; i < params.numWorkers; i++)
		wi[i].countries_num = 0;
	for (int i = 0; i < params.numWorkers; i++){ //na 8umi8w na kanw DELETE
		wi[i].countries_array = new char*[params.number_of_countries_per_worker];
		for (int j = 0; j < params.number_of_countries_per_worker; j++){
			wi[i].countries_array[j] = new char[100];
		}
	}
	int count = 0;
	for (int i = 0; i < list.counter; i++){
		if (i != 0 && (i%params.numWorkers) == 0)
			count++;
		strcpy(wi[i%params.numWorkers].countries_array[count],temp->country);
		wi[i%params.numWorkers].countries_num++;
		temp = temp->next;
	}
}

void Send_Message(int fd,char *message){ 
	int size = strlen(message); //size tou message pou 9a steilei o sender
	char temp[10]; //ftiaxnw ena temp
	sprintf(temp,"%d$",size); // to temp krataei to mege8os tou message px 1024$
	write(fd,temp,9); //kanw write sto fifo to 1024$
	write(fd,message,size); //kanw write to message
}

void Receive_Message(int fd){

	int bufferSize = params.bufferSize;
	char *input_message; // to message
	char temp[10]; //to mege8os tou message
	char *token; //8a apomonwsei ton ari8mo apo to dollario
	read(fd,temp,9); //diavazw to temp pou einai to mege8os tou message
	token = strtok(temp,"$"); //aponwnw ton ari8mo gia na 3erw posa bytes 8a diavasw
	int size = atoi(token); //te metatrepw se ari8mo
	input_message = (char*) malloc(size * sizeof(char) + 1); //o pinakas pou 8a valw mesa to message
	if (size < bufferSize)
		bufferSize = size;
	int counter = 0; //counter pou au3anetai mexri na ginei megalutero iso tou size
	int n = 0; //krataei posous xaraktites diavasame
	char *packet_ptr = input_message; //deixnei se poio simeio 8a gra4oume sto message
	while (counter < size){ //ekteleitai mexri na ginei megalutero iso tou size tou message
		packet_ptr += n; // einai i 8esi tou prwtou xaraktira pou 8a gra4oume sto epomeno minuma
		n = read(fd,packet_ptr,bufferSize); //oi xaraktires pou diavase
		//cout << "Disease n: "<< n << bufferSize << endl;
		if (counter + n > size){
			n = size - (counter + n);
		}
		counter += n; //au3anoume to counter n xaraktires
	}
	input_message[size] = 0; //kanoume to teleutaio xaraktira tou message 0
	if (!strcmp(input_message,""))
		delete []input_message;
	else{
		cout << input_message << endl;
		delete []input_message;
	}
}

void Send_Countries_to_Workers(Fifo *fifos,Workers_info *workers){
	char *message = NULL;
	for (int i = 0;i < params.numWorkers; i++){
		message = new char[sizeof(char)*100*workers[i].countries_num];
		memset(message,0,sizeof(char)*100*workers[i].countries_num);
		for (int j = 0;j < workers[i].countries_num; j++){
			sprintf(message,"%s%s$",message,workers[i].countries_array[j]);
		}
		Send_Message(fifos[i].filedesc,message);
		//cout << message << endl;
		delete []message;
	}
}
void Send_Commands_to_Workers(Fifo *fifos,char * command){

	for (int i = 0;i < params.numWorkers;i++){
		Send_Message(fifos[i].filedesc,command);
	}

}

void Initialize_already_read(Fifo *fifos){

	for (int i = 0;i < params.numWorkers;i++){
		fifos[i].already_read = 0;
	}

}

bool Exist_not_sent_Workers(Fifo *fifos){

	for (int i = 0;i < params.numWorkers;i++){
		if (fifos[i].already_read == 0)
			return true;
	}
	return false;

}

void Receive_Info_from_Workers(Fifo *fifos){

	struct  pollfd fdarray[params.numWorkers];
	Initialize_already_read(fifos);
	while (Exist_not_sent_Workers(fifos)){
		for (int i = 0;i < params.numWorkers; i++){ //arxikopoiisi tou fdarray
			fdarray[i].fd = fifos[i].filedesc; //dinw sto fd to filedesc tou antisoixou fifo
			fdarray[i].events = POLLIN; //kanw POLLIN na dw olous osous 8eloun na mou steiloun kati
		}
		poll(fdarray,params.numWorkers,10);
		for (int i = 0;i < params.numWorkers; i++){ //arxikopoiisi tou fdarray
			//an kati de douleuei swsta auto ftaiei
			if (fdarray[i].revents & POLLIN){
				Receive_Message(fdarray[i].fd);
				fifos[i].already_read = 1;
			}
		}
	}

}

void Termination_Statistics(Fifo *fifos_read,Fifo *fifos_write,Workers_info * wi){

	int pid,status;

	for (int i = 0;i < params.numWorkers; i++){
		kill(wi[i].pid,SIGKILL);
	}
	while((pid = wait(&status)) > 0);
	//aparaitites ektupwseis pou prepei na ginoun
	for (int i = 0; i < params.numWorkers; i++){
		close(fifos_read[i].filedesc);
		close(fifos_write[i].filedesc);
	}
	//an exei 8ema na valw se sxolio to close(fifos_write[i].filedesc);
	for (int i = 0; i < params.numWorkers; i++){
		unlink(fifos_read[i].fifo_name);
	}
	cout << "TERMINATING" << endl;
	exit(0);
	
	
}

// ./diseaseAggregator -w 2 -b 50 -i ../input_dir
int main(int argc,char **argv){
	getArgs(argc, argv);
    printArgs();
    int pid ;

	List list;
	Fifo fifo_read[params.numWorkers];
	Fifo fifo_write[params.numWorkers];
	Workers_info wi[params.numWorkers];

	Put_Countries_to_List(list,params.input_dir);
	Create_Fifos(fifo_read,fifo_write);
	Open_Fifos_Read(fifo_read); 
	Create_Workers_info(wi,list);

	for (int i = 0; i < params.numWorkers; i++){
		pid = fork();
		if (pid == 0){
			char buffersize[100];
			sprintf(buffersize,"%d",params.bufferSize);
			cout << fifo_write[i].fifo_name << endl;
			execl("../project1/diseaseMonitor","../project1/diseaseMonitor",buffersize,fifo_write[i].fifo_name,fifo_read[i].fifo_name,(char*) NULL);
		}
		else{
			wi[i].pid = pid;
		}
	}
	sleep(1);

	Open_Fifos_Write(fifo_write);
	Send_Countries_to_Workers(fifo_write,wi);

	Receive_Info_from_Workers(fifo_read);

	while (true){
		char *buffer = readCommands();
		if ( !strcmp(buffer,"/exit") ){
			delete []buffer;
			Termination_Statistics(fifo_read,fifo_write,wi);
		}
		Send_Commands_to_Workers(fifo_write,buffer);
		delete []buffer;
		Receive_Info_from_Workers(fifo_read);
	}


}