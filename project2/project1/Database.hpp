#ifndef Database_hpp
#define Database_hpp

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <iostream>
#include <set>
#include <utility>

using namespace std ;

class Date{
public:
	size_t day;
	size_t month;
	size_t year;
	size_t date_size; //o ari8mos apo twn imerwn pou exoun perasei apo to 01-01-0000
	Date(char *date);
	Date(){
		day = 0;
		month = 0;
		year = 0;
		date_size = 10000000;
	}
	/*
	Date & operator= (Date &d){
		day = d.day;
		month = d.month;
		year = d.year;
		date_size = d.date_size;
	}*/
};

class Case{  //edw apo8ikeuetai to ka8e simeio
public:
	int recordID;
	string patientFirstName;
	string patientLastName;
	string diseaseID;
	string country;
	string entry;
	string exit;
	Date entryDate;
	Date exitDate;
	int age;
	Case(string & rid,string & pfn,string & pln,string & did,string & c,string & entry,string & exit,string & patient_age){
		recordID = atoi(rid.c_str());
		patientFirstName = pfn;
		patientLastName = pln;
		diseaseID = did;
		country = c;
		this->entry = entry;
		this->exit = exit;
		Date d((char*)entry.c_str());
		entryDate = d;
		if (exit[0] == '-'){
			Date d1;
			exitDate = d1;
		}
		else{
			Date d1((char*)exit.c_str());
			exitDate = d1;	
		}
		age = atoi(patient_age.c_str());
	}

};

#endif /* Database_hpp */
