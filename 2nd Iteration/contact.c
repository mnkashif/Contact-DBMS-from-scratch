#include<stdio.h>
#include<stdlib.h>

#include "pds.h"
#include "contact.h"

void print_contact( struct Contact *c )
{
	printf("%d,%s,%s\n", c->contact_id,c->contact_name,c->phone);
}

// Use get_rec_by_key function to retrieve contact
int search_contact( int contact_id, struct Contact *c )
{
	int status;

	status = get_rec_by_key( contact_id, c );

	if( status == PDS_SUCCESS )
		status = CONTACT_SUCCESS;
	else
		status = CONTACT_FAILURE;

	return status;
}

// Add the given contact into the repository by calling put_rec_by_key
int add_contact( struct Contact *c )
{
	int status;

	status = put_rec_by_key( c->contact_id, c );

	if( status != PDS_SUCCESS ){
		fprintf(stderr, "Unable to add contact with key %d. Error %d", c->contact_id, status );
		status=CONTACT_FAILURE;
	}
	else
		status=CONTACT_SUCCESS;

	return status;
}



