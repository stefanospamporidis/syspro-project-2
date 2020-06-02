#!/bin/bash
EXPECTED_ARGS=5

if [ $EXPECTED_ARGS -ne $# ]
then
    echo wrong number of arguments - 5 needed
fi

diseasesFile=$1 #apo8ikeuw ta diseasesFile gia diavasma
countriesFile=$2 #apo8ikeuw to countriesFile gia diavasma
numFilesPerDirectory=$4 #o ari8mos twn subdirectories pou 8a kanw se ka8e xwra
numRecordsPerFile=$5 #o ari8mos twn eggrafwn pou 8a kanw se subdirectory input_dir/country/DD-MM-YYYY

newline=$'\n'

declare -a first_names
first_names=(James David Christopher George Ronald John Richard Daniel Kenneth Anthony Robert Charles Paul Steven Kevin Michael Joseph Mark Edward Jason William Thomas Donald Brian Jeff)
first_names+=(Mary Jennifer Lisa Sandra Michelle Patricia Maria Nancy Donna Laura Linda Susan Karen Carol Sarah Barbara Margaret Betty Ruth Kimberly Elizabeth Dorothy Helen Sharon Deborah)
#pinakas pou 8a epilegw random onomata
declare -a last_names
last_names=(Smith Anderson Clark Wright Mitchell Johnson Thomas Rodriguez Lopez Perez Williams Jackson Lewis Hill Roberts Jones White Lee Scott Turner Brown Harris Walker Green Phillips Davis Martin Hall Adams Campbell Miller Thompson Allen Baker Parker Wilson Garcia Young Gonzalez Evans Moore Martinez Hernandez Nelson Edwards Taylor Robinson King Carter Collins) 
#pinakas pou 8a epilegw epwnuma
declare -a enter_registrations
#pinakas pou krataw ta enter registrations ka8e eggrafes estw me vasi autes na ftiaxnw exit registrations 

declare -a diseases_array #pinakas pou 8a apo8ikeusw tis as8eneies wste na dialegw tuxaia apo ekei ka8e fora pou 8a dimiourgw mia eggrafi

counter=0 #krataei tis 8eseis pou 8a gemisw sto pinaka diseases_array
while read line; do
	# reading each line
	#echo $line
	diseases_array[counter]+="$line "
	counter=$(echo $(( $counter+1 ))) #enhmerwnw ton counter
done < $diseasesFile

number_of_countries=0 #krataei tis 8eseis pou 8a gemisw sto pinaka diseases_array
while read line; do
	# reading each line
	number_of_countries=$(echo $(( $number_of_countries+1 ))) #enhmerwnw ton counter
done < $countriesFile

total_records=$((number_of_countries*numFilesPerDirectory*numRecordsPerFile)) 
#krataei to sunolo twn eggrafwm pou 8a to xrisimopoiisw gia na ftia3w ta id twn eggrafwn

id=1 #ta id ta vazw me ti seira parola auta exw kai mia entoli pou tin kanw comment kai ta paragei randomly me vasi ton ari8mo twn sunolikwn eggrafwn
#an 8elete na tre3ete etsi apla kante comment auto kai to id++ kai uncomment ekeino pou ta kanw randomly
while read line; do
	# reading each line
	#echo $line
	#mkdir -p $3/$line

	#edw ftiaxnw ta enter
	c=0 #to c einai i 8esi sto pinaka pou vazw tin antistoixi eggrafi , gia ka8e xwra ousiastika o pinakas allazei kai pairnei tis 
	#enter eggrafes gia ti ka8e xwra
	#de kanw provle4i tis imerominias gia ta exit kai etsi mporei na dimiourgi8ei error, to afisa etsi wste na dw an to programma mou 8a 
	#mporouse na diaxeiristei kai ta errors kai episis giati den i3era an 8elate to script na paragei error i' oxi 
	for (( i=1; i<=$numFilesPerDirectory-($numFilesPerDirectory/3); i++ )) #ta 2/3 tou folder tis xwras einai enter eggrafes kai to upoloipo 1/3 exit eggrafes
	do 
		days=$(echo $((1 + RANDOM % 30))) #tuxaios ari8mos 1-30
		months=$(echo $((1 + RANDOM % 12))) #tuxaios ari8mos 1-12
		years=$(echo $((1900 + RANDOM % 121))) #tuxaios ari8mos 1900-2020
		file_data=""

		for (( j=1; j<=$numRecordsPerFile; j++ ))
		do 
			age=$(echo $((1 + RANDOM % 100))) #tuxaios ari8mos 1-100
			name_propability=$(echo $((RANDOM % 50)))
			surname_propability=$(echo $((RANDOM % 50)))
			disease_propability=$(echo $((RANDOM % ($counter))))
			#id=$(echo $((1 + RANDOM % ($total_records)))) #ftiaxnw ena id me vasi ton ari8mo twn eggrafwn pou 8a ftia3ei to programma
			id=$(echo $(( $id+1 ))) #enhmerwnw ton counter
			file_data+="$id ENTER ${first_names[name_propability]} ${last_names[surname_propability]} ${diseases_array[disease_propability]} $age $newline"
			enter_registrations[c]="$id EXIT ${first_names[name_propability]} ${last_names[surname_propability]} ${diseases_array[disease_propability]} $age"
			c=$(echo $(( $c+1 ))) #enhmerwnw ton counter
  		done

  		mkdir -p $3/$line #ftiaxnw to subdirectory input_dir/country/DD-MM-YYYY
  		echo  "$file_data" >> $3/$line/"$days-$months-$years"
  	done

  	#edw ftiaxnw ta exit
  	for (( i=1; i<=$numFilesPerDirectory/3; i++ ))
	do 
		days=$(echo $((1 + RANDOM % 30))) #tuxaios ari8mos 1-30
		months=$(echo $((1 + RANDOM % 12))) #tuxaios ari8mos 1-12
		years=$(echo $((1900 + RANDOM % 121))) #tuxaios ari8mos 1900-2020

		for (( j=1; j<=$numRecordsPerFile; j++ ))
		do 
			exit_propability=$(echo $((RANDOM % $c)))
			mkdir -p $3/$line #ftiaxnw to subdirectory input_dir/country/DD-MM-YYYY
  			echo  "${enter_registrations[exit_propability]}" >> $3/$line/"$days-$months-$years"
  		done	
  	done

done < $countriesFile



