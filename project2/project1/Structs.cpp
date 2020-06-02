#include "Structs.hpp"

void Sort_AgeRangesElement(AgeRangesElement *a){
	int i, j;
	AgeRangesElement temp;
	for (i = 0; i < 3; i++){
		for (j = i; j < 3; j++){
			if (a[j].counter <= a[j+1].counter){
				temp = a[j];
				a[j] = a[j+1];
				a[j+1] = temp;
			}
		}
	}
}


CaseListNode::CaseListNode(string & rid,string & pfn,string & pln,string & did,string & c,string & entry,string & exit,string & patient_age){ //constructor 
	patient = new Case(rid, pfn, pln, did, c, entry, exit,patient_age);
	next = NULL;
}


//return 1 la8os imerominia, return 2 idios kwdikos
int CasesList::AddNode(string & rid,string & pfn,string & pln,string & did,string & c,string & entry,string & exit,string & patient_age){ //pros8iki antikeimenou sti lista

	CaseListNode *newNode = new CaseListNode( rid, pfn, pln, did, c, entry, exit, patient_age);
	if (IsEmpty()){
		head = newNode;
		tail = newNode;
		counter = 1;
		return 0;
	}
	if (recordID_is_exist(newNode->patient->recordID)){
		delete newNode;
		return 2;
	}
	tail->next=newNode;
	tail = newNode;
	counter++;
	return 0;
}

void CasesList::UpdateNode(string & rid,string & exit){ //pros8iki antikeimenou sti lista

	CaseListNode *temp = head;
	Date exitDate((char*)exit.c_str());
	int flag = 0;
	while (temp != NULL){
		if (atoi(rid.c_str()) == temp->patient->recordID)
			//tsekarw an ola ta stoixeia tairiazoun 
			if (Wrong_Date(temp->patient->entryDate,exitDate) == false ){
				temp->patient->exit = exit;
				temp->patient->exitDate = exitDate;
			}	
		temp = temp->next;
	}

}

	
CasesList::~CasesList(){ //destructor
	CaseListNode *temp;
	while(head){
		temp = head;
		head = head->next;
		delete temp;
	}
}
	
bool CasesList::recordID_is_exist(int rid){ //vriskei an to recordid pou tou dwsame uparxei idi sti lista
	CaseListNode *temp = head;
	while (temp != NULL){
		if (rid == temp->patient->recordID)
			return true;
		temp = temp->next;
	}
	return false;
}

char* CasesList::searchPatientRecord(int rid){ //vriskei an to recordid pou tou dwsame uparxei idi sti lista
	CaseListNode *temp = head;
	string registration = "";
	char * results = new char[1024];
	while (temp != NULL){
		if (rid == temp->patient->recordID){
			//return true;
			registration = to_string(temp->patient->recordID)+" "+temp->patient->patientFirstName+" "+temp->patient->patientLastName+" "+temp->patient->diseaseID+" "+temp->patient->country+" "+to_string(temp->patient->age)+" "+temp->patient->entry+" "+temp->patient->exit;
		}
		temp = temp->next;
	}
	strcpy(results,registration.c_str());
	return results;
	delete []results;
}
	
bool CasesList::Wrong_Date(Date entryDate,Date exitDate){
	return entryDate.date_size > exitDate.date_size; //epistrefei False an einai swsto
}


BucketMember::BucketMember(){ // contsructor

	avl_tree = new AVL_tree();

}

BucketMember::~BucketMember(){ //destructor
	delete avl_tree;
}


BucketList::~BucketList(){ //destructor 
	Bucket *bucket;
	for (int i=0;i < counter; i++){
		bucket = head;
		head = head->next;
		delete bucket;
	}
}

bool BucketList::isExist(string disease_country,int key_date,CaseListNode *patient_node){ //an i as8eneia i' i xwra uparxei idi sto bucket tote to pros8etei sti swsti 8esi kai 
	//epistrefei true alliws false
	Bucket *bucket = head;
	int i;
	while (bucket){
		for (i = 0;i < bucket->occupied;i++){
			if (bucket->bm[i].key_id == disease_country){
				bucket->bm[i].avl_tree->insert(key_date,patient_node);
				return true;
			}
		}
		bucket = bucket->next;
	}
	return false;
}

char* BucketList::listCountries(){

	char* results = new char[1024];
	string temp = "";
	Bucket *bucket = head;
	int i;
	while (bucket){
		for (i = 0;i< bucket->occupied; i++){
			//cout << bucket->bm[i].key_id << endl;
			temp += string(bucket->bm[i].key_id) + " " + to_string(getpid()) +"\n";
		}
		bucket = bucket->next;
	}
	strcpy(results,temp.c_str());
	return results;
	delete []results;
}

char* BucketList::diseaseFrequency(string virus,int date_size1,int date_size2,string country){

	char* results = new char[1024];
	string temp="";
	Bucket *bucket = head;
	int i;
	int counter = 0;
	while (bucket){
		for (i = 0;i< bucket->occupied;i++){
			if (bucket->bm[i].key_id == virus){
				if (country == ""){
					temp += bucket->bm[i].key_id + " " + to_string(bucket->bm[i].avl_tree->get_patient_frequency(date_size1, date_size2));
				}																
				else{
					if (bucket->bm[i].avl_tree->get_patient_frequency(date_size1, date_size2, country) != 0) //an i xwra uparxei mesa sti vasi mono tote 8a stelnete minuma me to result
						temp += bucket->bm[i].key_id + " " + to_string(bucket->bm[i].avl_tree->get_patient_frequency(date_size1, date_size2, country));	
				}																 
				counter = 1;
			}
		}
		bucket = bucket->next;
	}
	strcpy(results,temp.c_str());
	return results;
	delete []results;
}

char* BucketList::topkAgeRanges(int k,string country,string disease,int date_size1,int date_size2){
	char *results = new char[1024];
	string temp = "";
	Bucket *bucket = head;
	int i,j;
	double total = 0;
	AgeRangesElement agesfrequency[4];
	while (bucket){
		for (i = 0;i < bucket->occupied; i++){
			if (bucket->bm[i].key_id == country){
				bucket->bm[i].avl_tree->AgeRanges_disease_country(agesfrequency,disease,date_size1,date_size2);
				Sort_AgeRangesElement(agesfrequency);
				for (j = 0; j < 4; j++){
					total += agesfrequency[j].counter;
				}
				if (k > 4) 
					k = 4; 
				for (j = 0; j < k; j++){
					if (agesfrequency[j].age_range == 0)
						temp += "0-20: " + to_string(agesfrequency[j].counter * 100 / total) + "%\n";
					else if (agesfrequency[j].age_range == 1)
						temp += "21-40: " + to_string(agesfrequency[j].counter * 100 / total) + "%\n";
					else if (agesfrequency[j].age_range == 2)
						temp += "41-60: " + to_string(agesfrequency[j].counter * 100 / total) + "%\n";
					else
						temp += "60+: " + to_string(agesfrequency[j].counter * 100 / total) + "%\n";
				}
			}
		}
		bucket = bucket->next;
	}
	strcpy(results,temp.c_str());
	return results;
	delete []results;
}

char* BucketList::numPatientAdmissions(string disease,int date1,int date2,string country){

	char *results = new char[1024];
	string temp = "";
	Bucket *bucket = head;
	int i;
	int counter = 0;
	if (country != ""){
		while (bucket){
			for (i = 0;i< bucket->occupied; i++){
				if (bucket->bm[i].key_id == country){
					//vriskei ta cases sto avl tree pou den eixan exit date
					temp += bucket->bm[i].key_id + " " + to_string(bucket->bm[i].avl_tree->get_patient_admisions(date1,date2,disease));
					//cout << bucket->bm[i].key_id << " " << bucket->bm[i].avl_tree->get_patient_admisions(date1,date2,disease) <<endl; 
				}
			}
			bucket = bucket->next;
		}
	}
	else {
		while (bucket){
			for (i = 0;i< bucket->occupied; i++){
				temp += bucket->bm[i].key_id + " " + to_string(bucket->bm[i].avl_tree->get_patient_admisions(date1,date2,disease)) + "\n";
				//cout << bucket->bm[i].key_id << " " << bucket->bm[i].avl_tree->get_patient_admisions(date1,date2,disease) << endl;
			}
			bucket = bucket->next;
		}
	}
	
	strcpy(results,temp.c_str());
	return results;
	delete []results;
}

char* BucketList::numPatientDischarges(string disease,int date1,int date2,string country){

	char *results = new char[1024];
	string temp = "";
	Bucket *bucket = head;
	int i;
	if (country != ""){
		while (bucket){
			for (i = 0;i< bucket->occupied; i++){
				if (bucket->bm[i].key_id == country){
					//vriskei ta cases sto avl tree pou den eixan exit date
					temp += bucket->bm[i].key_id + " " + to_string(bucket->bm[i].avl_tree->get_discharged_patient(date1,date2,disease)) + "\n";
					//cout << bucket->bm[i].key_id << " " << bucket->bm[i].avl_tree->get_discharged_patient(date1,date2,disease) <<endl; 
				}
			}
			bucket = bucket->next;
		}
	}
	else {
		while (bucket){
			for (i = 0;i< bucket->occupied; i++){
				temp += bucket->bm[i].key_id + " " + to_string(bucket->bm[i].avl_tree->get_discharged_patient(date1,date2,disease)) + "\n";
				//cout << bucket->bm[i].key_id << " " << bucket->bm[i].avl_tree->get_discharged_patient(date1,date2,disease) << endl;
			}
			bucket = bucket->next;
		}
	}
	
	strcpy(results,temp.c_str());
	return results;
	delete []results;
}


int BucketList::insert_item(string disease_country,int key_date,CaseListNode *patient_node){

	if (isEmpty()){ //an einai adeia tin arikopoiw kai vazw to prwto stoixeio
		Bucket *bucket = new Bucket(Bucket_member_size);
		bucket->bm[bucket->occupied].key_id = disease_country;
		bucket->bm[bucket->occupied].avl_tree->insert(key_date,patient_node);
		bucket->occupied += 1; // i prwti eleu8eri 8esi enos pinaka apo BucketMembers
		counter += 1; //posa buckets exei i lista
		head = bucket;
		tail = bucket;
		return 1;
	}
	if (isExist(disease_country,key_date,patient_node)){
		return 0;
	}
	if (tail->Bucket_member_size == tail->occupied){ //an exei gemisei to Bucket apla ftiaxnei kainourio kai to pros8etei se ekeino
		Bucket *bucket = new Bucket(Bucket_member_size);
		bucket->bm[bucket->occupied].key_id = disease_country;
		bucket->bm[bucket->occupied].avl_tree->insert(key_date,patient_node);
		bucket->occupied += 1;
		counter += 1;
		tail->next = bucket;
		tail = bucket;
		return 1;
	}
	tail->bm[tail->occupied].key_id = disease_country; //an de mpei sta alla simainei oti vrike bucket pou eixe xwro kai mpike sti prwti adeia 8esi
	tail->bm[tail->occupied].avl_tree->insert(key_date,patient_node);
	tail->occupied += 1;
	return 1;

}

int Hashtable::Hash_function(string str){ //universal hash function me string

	int h = 0;
	int a = 6;
	int p = 7919;
	int i = 0;
	while (str[i]){
		h = ( h*a+str[i] ) % p;
		i++;
	}
	return ( h%length );

}

Hashtable::Hashtable(int length,int Bucket_member_size){ //constructor
	this->length = length;
	items_counter = 0;
	hashtable = new BucketList*[length];
	for (int i = 0;i < length; i++){
		hashtable[i] = new BucketList(Bucket_member_size);
	}
}

void Hashtable::insert_item(string disease_country,int key_date,CaseListNode *patient_node){ //vazei ta item

	int position = Hash_function(disease_country);
	items_counter += hashtable[position]->insert_item(disease_country,key_date,patient_node);

}

char* Hashtable::listCountries(){
	string temp = "";
	char *results = new char[1024];
	for (int i = 0;i < length; i++){
		temp += string(hashtable[i]->listCountries());
	}
	strcpy(results,temp.c_str());
	return results;
	delete []results;
}


char* Hashtable::diseaseFrequency(string virus,int date_size1,int date_size2,string country){ //kalei ti diseaseFrequency pou ekteleitai parapanw

	char *results = new char[1024];
	int position = Hash_function(virus);
	strcpy(results,hashtable[position]->diseaseFrequency(virus,date_size1,date_size2,country));
	return results;
	delete []results;

}

char* Hashtable::topkAgeRanges(int k,string country,string disease,int date_size1,int date_size2){ //kalei ti topkAgeRanges pou ekteleitai parapanw

	char *results = new char[1024];
	int position = Hash_function(country);
	strcpy(results,hashtable[position]->topkAgeRanges(k,country,disease,date_size1,date_size2));
	return results;
	delete []results;
}

char* Hashtable::numPatientAdmissions(string disease,int date1,int date2,string country){
	string temp = "";
	char *results = new char[1024];
	if (country != ""){
		int position = Hash_function(country);
		strcpy(results,hashtable[position]->numPatientAdmissions(disease, date1, date2, country));
	}
	else {
		for (int i = 0;i < length; i++){
			temp += string(hashtable[i]->numPatientAdmissions(disease, date1, date2, country));
		}
		strcpy(results,temp.c_str());
	}
	return results;
	delete []results;
}

char* Hashtable::numPatientDischarges(string disease,int date1,int date2,string country){
	string temp = "";
	char *results = new char[1024];
	if (country != ""){
		int position = Hash_function(country);
		strcpy(results,hashtable[position]->numPatientDischarges(disease, date1, date2, country));
	}
	else {
		for (int i = 0;i < length; i++){
			temp += string(hashtable[i]->numPatientDischarges(disease, date1, date2, country));
		}
		strcpy(results,temp.c_str());
	}
	return results;
	delete []results;
}


Hashtable::~Hashtable(){ //Destructor
	for (int i = 0;i < length; i++){
		delete hashtable[i];
	}
	delete [] hashtable;
}