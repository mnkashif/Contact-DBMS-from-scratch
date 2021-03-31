#include<stdio.h>
#include<stdlib.h>

#include "pds.h"
#include "contact.h"

main()
{
	char *datafile = "contact_info.txt";
	char *repo_name = "demo";
	int status, rec_size;

	rec_size = sizeof(struct Contact);

	status = pds_open( repo_name, rec_size );
	if( status != PDS_SUCCESS ){
		fprintf(stderr, "Unable to open repository %s - Error %d\n", repo_name, status );
		exit(status);
	}

	store_contacts( datafile );
	pds_close();
}


