#include "List.hpp"

void List::AddNode(char *country){ //pros8iki antikeimenou sti lista

	ListNode *newNode = new ListNode(country);
	if (IsEmpty()){
		head = newNode;
		tail = newNode;
		counter = 1;
		return;
	}
	tail->next=newNode;
	tail = newNode;
	counter++;
	return;

}


List::~List(){ //destructor
	ListNode *temp;
	while(head){
		temp = head;
		head = head->next;
		delete temp;
	}
}