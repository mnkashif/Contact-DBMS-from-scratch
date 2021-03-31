#include <stdlib.h>
#include <stdio.h>
#include<string.h>
#include"pds.h"
// #include"bst.h"
#include<errno.h>

struct PDS_DB_Master db_handle;

int pds_create_schema (char *schema_file_name)
{
	char a[30];
	strcpy(a,schema_file_name);
	FILE *p=fopen(a,"rb+");
	struct PDS_DBInfo db_info;
	fscanf(p,"%s",db_info.db_name);
	struct PDS_EntityInfo entity;
	struct PDS_LinkInfo link;
	int m=0,n=0;
	int c;
	while(!feof(p))
	{
		fscanf(p,"%s",a);
		if(!strcmp(a,"entity"))
		{
			fscanf(p,"%s",a);
			strcpy(entity.entity_name,a);
			fscanf(p,"%d",&c);
			entity.entity_size=c;
			db_info.entities[m]=entity;
			m++;
		}
		else if(!strcmp(a,"relationship"))
		{
			fscanf(p,"%s",a);
			strcpy(link.link_name,a);
			fscanf(p,"%s",a);
			strcpy(link.pds_name1,a);
			fscanf(p,"%s",a);
			strcpy(link.pds_name2,a);
			db_info.links[n]=link;
			n++;
		}
	}
	db_info.num_entities=m;
	db_info.num_relationships=n;
	fclose(p);

	strcpy(a,db_info.db_name);
	strcat(a,".db");
	// printf("%s\n",a);

	p=fopen(a,"wb+");
	fwrite(&db_info,sizeof(struct PDS_DBInfo),1,p);
	fclose(p);
	return PDS_SUCCESS;
}
	
// P01) pds_db_open:
// Open the database info file. The file is named with .db extension.
// Example: If db_name is "bank", then file name is "bank.db"
// This should populate the PDS_DB_Master global variable named db_handle;
int pds_db_open( char *db_name )
{
	// strcat(db_name,".db");
	// if(db_handle.db_status==PDS_DB_OPEN)
	// 	return PDS_DB_ALREADY_OPEN;
	// FILE *p=fopen(db_name,"rb+");
	// db_handle.db_status=PDS_DB_OPEN;
	// fread(db_handle.db_info,sizeof(struct PDS_DBInfo),1,p);
	// struct PDS_RepoInfo entity_info;
	// for(int i=0;i<MAX_ENTITY;i++)
	// {



	// }









}

// P02) pds_open
// Open the data file and index file in rb+ mode
// Update the fields of PDS_RepoInfo appropriately
// Build BST and store in pds_bst by reading index entries from the index file
// Close only the index file
int pds_open( char *repo_name, int rec_size )
{}

// P03) pds_load_ndx
// Internal function used by pds_open to read index entries into BST
int pds_load_ndx( struct PDS_RepoInfo *repo_handle )
{}

// P04) put_rec_by_key
// Retrieve the repo_handle from PDS_DB_Master corresponding to given entity_name
// Create an index entry with the current data file location using ftell
// Add index entry to BST using offset returned by ftell
// Check if there are any deleted location available in the free list
// If available, seek to the free location, other seek to the end of the file
// 1. Write the key at the current data file location
// 2. Write the record after writing the key
int put_rec_by_key( char *entity_name, int key, void *rec )
{}

// P05) get_rec_by_key
// Retrieve the repo_handle from PDS_DB_Master corresponding to given entity_name
// Search for index entry in BST
// Seek to the file location based on offset in index entry
// 1. Read the key at the current file location 
// 2. Read the record after reading the key
int get_rec_by_ndx_key( char *entity_name, int key, void *rec )
{}

// P06) get_rec_by_non_ndx_key
// Retrieve the repo_handle from PDS_DB_Master corresponding to given entity_name
// Search based on a key field on which an index 
// does not exist. This function actually does a full table scan 
// by reading the data file until the desired record is found.
// The io_count is an output parameter to indicate the number of records
// that had to be read from the data file for finding the desired record
int get_rec_by_non_ndx_key( 
char *entity_name, /* The entity file from which data is to be read */
void *key,  			/* The search key */
void *rec,  			/* The output record */
int (*matcher)(void *rec, void *key), /*Function pointer for matching*/
int *io_count  		/* Count of the number of records read */
)
{}

// P07) update
// Retrieve the repo_handle from PDS_DB_Master corresponding to given entity_name
// Search for index entry in BST
// Seek to the file location based on offset in index entry
// Overwrite the existing record with the given record
// In case of any error, return PDS_MODIFY_FAILED
int update_by_key( char *entity_name, int key, void *newrec )
{}

// P08) pds_delete
// Retrieve the repo_handle from PDS_DB_Master corresponding to given entity_name
// Search for index entry in BST
// store the offset value in to free list
int delete_by_key( char *entity_name, int key )
{}

// P09) link_objects
// Retrieve the link_info from PDS_DB_Master corresponding to given link_name
// Adds a link entry to the link file establishing link between two given keys
// Example: link_data("student_course", 12, 15);
// Give error key1 does not exist or key2 does not exist or <key, linke_key> combination already exists
int link_data(char *link_name, int key, int linked_key)
{}

// P10) getLinkedData
// Returns the linked data for a given key
// Example: Retrieve all courses (linked_data) done by a given student (data_key)
int get_linked_data( char *link_name, int data_key, struct PDS_LinkedKeySet *linked_data )
{}

// P11) pds_close
// Retrieve the repo_handle from PDS_DB_Master corresponding to given entity_name
// Open the index file in wb mode (write mode, not append mode)
// Unload the BST into the index file by traversing it in PRE-ORDER (overwrite the entire index file)
// Free the BST by call bst_destroy()
// Close the index file and data file
int pds_close( char *entity_name )
{}

// P12) pds_db_close
// Close all the entity files by calling pds_close
// Close all link files
int pds_db_close()
{}