#ifndef AVL_Heap_hpp
#define AVL_Heap_hpp

#include <stdio.h>
#include <string>
#include <iostream>
#include <set>
#include <utility>
#include <math.h>
#include "Database.hpp"
#include "Structs.hpp"

using namespace std;

class CaseListNode;

struct AgeRangesElement;

class AVL_node{
public:
    size_t entry_date;
    AVL_node *left;
    AVL_node *right;
    int height;
    CaseListNode *patient_node;
    AVL_node(size_t entry_date, CaseListNode *patient_node){
        this->entry_date = entry_date;
        left = NULL;
        right = NULL;
        height = 1;
        this->patient_node = patient_node;
    }
    ~AVL_node(){}

};

class AVL_tree{
public:
    AVL_node *root;
    size_t counter; //to pli8os twn komvwn tou dentrou
    AVL_tree(){
        root = NULL;
        counter = 0;
    }
    ~AVL_tree();
    void delete_recursive(AVL_node * root);
    void insert(size_t entry_date,CaseListNode *patient_node);
    int node_counter(AVL_node* node,size_t date_size1,size_t date_size2);
    int get_patient_frequency(size_t date_size1,size_t date_size2);

    int node_counter(AVL_node* node,size_t date_size1,size_t date_size2,string country);
    int get_patient_frequency(size_t date_size1,size_t date_size2,string country);

    void AgeRanges_disease_country(AgeRangesElement *agesfrequency,string disease,size_t date_size1,size_t date_size2);
    void AgeRanges_disease_country_recursive(AVL_node* root,string disease,AgeRangesElement *agesfrequency,size_t date_size1,size_t date_size2);

    int patients_counter(AVL_node* node,size_t date_size1,size_t date_size2,string disease);
    int get_patient_admisions(size_t date_size1,size_t date_size2,string disease);

    int discharged_patients_counter(AVL_node* node,size_t date_size1,size_t date_size2,string disease);
    int get_discharged_patient(size_t date_size1,size_t date_size2,string disease);

    AVL_node* insert(AVL_node* node, size_t entry_date,CaseListNode *patient_node);
    int getBalance(AVL_node* node);
    AVL_node* leftRotate(AVL_node* node);
    AVL_node* rightRotate(AVL_node* node);
    int max(int a, int b);
    int height(AVL_node* node);

};



#endif
