#include "AVL_Heap.hpp"

//epistrefei ta krousmata pou vre8ikan anamesa stis imerominies, an de tirountai oi proupo8eseis apla gurnaei 0 kai sunexizei tin anadromi
int AVL_tree::node_counter(AVL_node* node,size_t date_size1,size_t date_size2){
 
	if (node == NULL)
		return 0;
    else if (node->entry_date < date_size1)
        return 0 + node_counter(node->right,date_size1,date_size2);
    else if (node->entry_date > date_size2)
        return 0 + node_counter(node->left,date_size1,date_size2);
    else
	    return 1 + node_counter(node->left,date_size1,date_size2) + node_counter(node->right,date_size1,date_size2);

}


int AVL_tree::get_patient_frequency(size_t date_size1,size_t date_size2){

	return node_counter(root,date_size1,date_size2);

}

//to idio me parapanw apla twra koitaei kai ti xwra tous otan pros8etei ta cases
int AVL_tree::node_counter(AVL_node* node,size_t date_size1,size_t date_size2,string country){

    if (node == NULL)
        return 0;
    else if (node->entry_date < date_size1)
        return 0 + node_counter(node->right,date_size1,date_size2,country);
    else if (node->entry_date > date_size2)
        return 0 + node_counter(node->left,date_size1,date_size2,country);
    else
        if (node->patient_node->patient->country == country)
            return 1 + node_counter(node->left,date_size1,date_size2,country) + node_counter(node->right,date_size1,date_size2,country);
        else
            return 0 + node_counter(node->left,date_size1,date_size2,country) + node_counter(node->right,date_size1,date_size2,country);

}


int AVL_tree::get_patient_frequency(size_t date_size1,size_t date_size2,string country){

    return node_counter(root,date_size1,date_size2,country);

}


void AVL_tree::AgeRanges_disease_country_recursive(AVL_node* root,string disease,AgeRangesElement *agesfrequency,size_t date_size1,size_t date_size2){

    if (root == NULL)
        return;
    if (root->entry_date >= date_size1 && root->entry_date <= date_size2){
        if (root->patient_node->patient->diseaseID == disease){
            if (root->patient_node->patient->age >= 0 && root->patient_node->patient->age < 21)
                agesfrequency[0].counter++;
            else if (root->patient_node->patient->age >= 21 && root->patient_node->patient->age < 41)
                agesfrequency[1].counter++;
            else if (root->patient_node->patient->age >= 41 && root->patient_node->patient->age < 61)
                agesfrequency[2].counter++;
            else
                agesfrequency[3].counter++;
        }
    }
    AgeRanges_disease_country_recursive(root->left,disease,agesfrequency,date_size1,date_size2);
    AgeRanges_disease_country_recursive(root->right,disease,agesfrequency,date_size1,date_size2);    


}


void AVL_tree::AgeRanges_disease_country(AgeRangesElement *agesfrequency,string disease,size_t date_size1,size_t date_size2){

    for (int i = 0; i < 4; i++){
        agesfrequency[i].counter = 0;
        agesfrequency[i].age_range = i; //i agesfrequency[0].age_range einai i 0-20,agesfrequency[1].age_range einai i 20-40
    }   //agesfrequency[2].age_range einai i 40-60 kai i agesfrequency[3].age_range einai i 60++
    AgeRanges_disease_country_recursive(root,disease,agesfrequency,date_size1,date_size2);
    
}


int AVL_tree::patients_counter(AVL_node* node,size_t date_size1,size_t date_size2,string disease){

    if (node == NULL)
        return 0;
    else if (node->entry_date < date_size1)
        return 0 + patients_counter(node->right,date_size1,date_size2,disease);
    else if (node->entry_date > date_size2)
        return 0 + patients_counter(node->left,date_size1,date_size2,disease);
    else
        if (node->patient_node->patient->diseaseID == disease)
            return 1 + patients_counter(node->left,date_size1,date_size2,disease) + patients_counter(node->right,date_size1,date_size2,disease);
        else
            return 0 + patients_counter(node->left,date_size1,date_size2,disease) + patients_counter(node->right,date_size1,date_size2,disease);

}


int AVL_tree::get_patient_admisions(size_t date_size1,size_t date_size2,string disease){

    return patients_counter(root,date_size1,date_size2,disease);

}

int AVL_tree::discharged_patients_counter(AVL_node* node,size_t date_size1,size_t date_size2,string disease){

    if (node == NULL)
        return 0;
    else if (node->entry_date < date_size1)
        return 0 + discharged_patients_counter(node->right,date_size1,date_size2,disease);
    else if (node->entry_date > date_size2)
        return 0 + discharged_patients_counter(node->left,date_size1,date_size2,disease);
    else
        if (node->patient_node->patient->diseaseID == disease && node->patient_node->patient->exitDate.date_size == 10000000)
            return 1 + discharged_patients_counter(node->left,date_size1,date_size2,disease) + discharged_patients_counter(node->right,date_size1,date_size2,disease);
        else
            return 0 + discharged_patients_counter(node->left,date_size1,date_size2,disease) + discharged_patients_counter(node->right,date_size1,date_size2,disease);

}


int AVL_tree::get_discharged_patient(size_t date_size1,size_t date_size2,string disease){

    return discharged_patients_counter(root,date_size1,date_size2,disease);

}



//kalei tin insert na valei case sto dentro
void AVL_tree::insert(size_t entry_date,CaseListNode *patient_node){
    if (counter != 0)
	root = insert(root,entry_date,patient_node);
	counter += 1;

}


AVL_node* AVL_tree::insert(AVL_node* node, size_t entry_date, CaseListNode *patient_node){

	/* 1. Perform the normal BST insertion */
    if (node == NULL)  
        return(new AVL_node(entry_date, patient_node));  
  
    if (entry_date <= node->entry_date)  
        node->left = insert(node->left, entry_date, patient_node);  
    else if (entry_date > node->entry_date)  
        node->right = insert(node->right, entry_date, patient_node);    
  
    /* 2. Update height of this ancestor node */
    node->height = 1 + max(height(node->left),  
                        height(node->right));  
  
    /* 3. Get the balance factor of this ancestor  
        node to check whether this node became  
        unbalanced */
    int balance = getBalance(node);  
  
    // If this node becomes unbalanced, then  
    // there are 4 cases  
  
    // Left Left Case  
    if (balance > 1 && entry_date <= node->left->entry_date)  
        return rightRotate(node);  
  
    // Right Right Case  
    if (balance < -1 && entry_date > node->right->entry_date)  
        return leftRotate(node);  
  
    // Left Right Case  
    if (balance > 1 && entry_date > node->left->entry_date)  
    {  
        node->left = leftRotate(node->left);  
        return rightRotate(node);  
    }  
  
    // Right Left Case  
    if (balance < -1 && entry_date <= node->right->entry_date)  
    {  
        node->right = rightRotate(node->right);  
        return leftRotate(node);  
    }  
  

    /* return the (unchanged) node pointer */
    return node; 

}


int AVL_tree::getBalance(AVL_node* node){

	return node == NULL ? 0 : height(node->left) - height(node->right) ;

}
    

AVL_node* AVL_tree::leftRotate(AVL_node* node){

	AVL_node *y = node->right;  
    AVL_node *x = y->left;  
  
    // Perform rotation  
    y->left = node;  
    node->right = x;  
  
    // Update heights  
    node->height = max(height(node->left),     
                    height(node->right)) + 1;  
    y->height = max(height(y->left),  
                    height(y->right)) + 1;  
  
    // Return new root  
    return y; 

}



AVL_node* AVL_tree::rightRotate(AVL_node* node){

	AVL_node *x = node->left;  
    AVL_node *y = x->right;  
  
    // Perform rotation  
    x->right = node;  
    node->left = y;  
  
    // Update heights  
    node->height = max(height(node->left), 
                    height(node->right)) + 1;  
    x->height = max(height(x->left), 
                    height(x->right)) + 1;  
  
    // Return new root  
    return x;  

}


//vriskei to megisto anamesa se 2 times kai tin epistrefei
int AVL_tree::max(int a, int b){

	return a > b ? a : b ;
    
}


//vriskei to u4os tou dentrou kai to epistrefei
int AVL_tree::height(AVL_node* node){

	return node == NULL ? 0 : node->height ;

}

//destructor
AVL_tree::~AVL_tree(){

	delete_recursive(root);

}

void AVL_tree::delete_recursive(AVL_node * root){

	if (root == NULL){
		return;
	}
	delete_recursive(root->left);
	delete_recursive(root->right);
	delete root;

}

