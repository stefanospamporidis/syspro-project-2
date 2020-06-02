#ifndef List_hpp
#define List_hpp

#include <stdio.h>
#include <string.h>
#include <iostream>

class ListNode{
public:
	char country[100];
	ListNode *next;
	ListNode(char *country){
		strcpy(this->country,country);
		next = NULL;
	}
	~ListNode(){}
};

class List{
public:
	ListNode *head;
	ListNode *tail;
	int counter;
	List(){
		head = NULL;
		tail = NULL;
		counter = 0;
	}
	~List();
	void AddNode(char *country);
	bool IsEmpty(){
		return counter == 0;
	}
};


#endif


/*for (( j=1; j<=numRecordsPerFile; j++))
do
  	r=$(echo $((3 + RANDOM % 10))) #tuxaios ari8mos 3-12
    name=$(tr -cd '[:alnum:]'< /dev/urandom | head -c $r) #kanw random name apo 3b mexri 12b
    surname=$(tr -cd '[:alnum:]'< /dev/urandom | head -c $r) #kanw random name apo 3b mexri 12b
    echo $name >> $mydir/"$days-$months-$years" ##ftiaxnw to subdirectory input_dir/country/DD-MM-YYYY kai grafw sto file DD-MM-YYYY
    id_counter=$(echo $(( $id_counter+1 ))) #enhmerwnw ton counter twn id
done*/