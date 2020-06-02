#ifndef Structs_hpp
#define Structs_hpp

#include <stdio.h>
#include <string>
#include <iostream>
#include <set>
#include <utility>
#include <unistd.h>
#include "Database.hpp"
#include "AVL_Heap.hpp"

using namespace std;

typedef struct AgeRangesElement
{
	int counter;
	int age_range;
}AgeRangesElement;

void Sort_AgeRangesElement(AgeRangesElement *agesfrequency);

class CaseListNode{
public:
	Case *patient;
	CaseListNode *next;
	CaseListNode(string & rid,string & pfn,string & pln,string & did,string & c,string & entry,string & exit,string & patient_age);
	~CaseListNode(){
		delete patient;
	}
};

class CasesList{
public:
	CaseListNode *head;
	CaseListNode *tail;
	int counter;
	CasesList(){
		head = NULL;
		tail = NULL;
		counter = 0;
	}
	int AddNode(string & rid,string & pfn,string & pln,string & did,string & c,string & entry,string & exit,string & patient_age);
	void UpdateNode(string & rid,string & exit);
	int recordPatientExit(int recordID,Date exitDate);
	~CasesList();
	bool recordID_is_exist(int rid);
	char* searchPatientRecord(int rid);
	bool Wrong_Date(Date entryDate,Date exitDate);
	bool IsEmpty(){
		return counter == 0;
	}

};

class AVL_tree;

class BucketMember{
public:
	string key_id; //8a einai eite to onoma kapoiou iou-as8eneias i' to onoma kapoias xwras
	AVL_tree *avl_tree;

	BucketMember();
	~BucketMember();

};

class Bucket{
public:
	int Bucket_member_size;
	int occupied;
	BucketMember *bm;
	Bucket *next;
	Bucket(int size){
		Bucket_member_size = size;
		occupied = 0;
		bm = new BucketMember[size];
		next = NULL;
	}
	~Bucket(){
		delete [] bm;
	}

};

class BucketList{
public:
	int counter;
	int Bucket_member_size;
	Bucket *head;
	Bucket *tail;
	BucketList(int size){
		Bucket_member_size = size;
		head = NULL;
		tail = NULL;
		counter = 0;
	}
	int insert_item(string disease_country,int key_date,CaseListNode *patient_node);
	bool isEmpty(){
		return counter == 0;
	}
	bool isExist(string disease_country,int key_date,CaseListNode *patient_node);
	char* listCountries();
	char* diseaseFrequency(string virus,int date_size1,int date_size2,string country);
	char* topkAgeRanges(int k,string country,string disease,int date_size1,int date_size2);
	char* numPatientAdmissions(string disease,int date1,int date2,string country);
	char* numPatientDischarges(string disease,int date1,int date2,string country);
	~BucketList();

};

class Hashtable{
public:
	int length;
	int items_counter;
	BucketList **hashtable;
	Hashtable(int length,int Bucket_member_size);
	char* listCountries();
	char* diseaseFrequency(string virus,int date_size1,int date_size2,string country = "");
	char* topkAgeRanges(int k,string country,string disease,int date_size1,int date_size2);
	char* numPatientAdmissions(string disease,int date1,int date2,string country = "");
	char* numPatientDischarges(string disease,int date1,int date2,string country = "");
	int Hash_function(string str);
	void insert_item(string disease_country,int key_date,CaseListNode *patient_node); //pairnei imerominia i' xwra analoga me to hashtable,to key pou einai to date_size 
	~Hashtable();																//kai to caselistnode *patient_code gia na exei prosvasi sta dedomena tou as8eni

};

#endif