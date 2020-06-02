#include "Database.hpp"

Date::Date(char *date){ //metatrepw ta Date se Datesize opou to datesize einai oi meres pou exoun perasei apo to 00/00/0000 an den exei apla to vazw kati terastio wste na 
	string s;
	s = string(date); //ikanopoiei tis diafores sun8ikes pou xreizontai meta3u entry date kai exit date
	string delimiter = "-";

    size_t pos = 0;
    string token;
    pos = s.find(delimiter);
    token = s.substr(0, pos);
    day = atoi(token.c_str());
    s.erase(0, pos + delimiter.length());
    pos = s.find(delimiter);
    token = s.substr(0, pos);
    month = atoi(token.c_str());
    s.erase(0, pos + delimiter.length());
    pos = s.find(delimiter);
    token = s.substr(0, pos);
    year = atoi(token.c_str());
    s.erase(0, pos + delimiter.length());

	date_size = (year*365) + (month*30) + day;

}