#include <stdlib.h>
#include <stdio.h>
#include<string.h>
#include"pds.h"
#include"bst.h"
#include<errno.h>
#define PDS_LINK_OPEN 15
#define PDS_LINK_CLOSED 16
struct PDS_DB_Master db_handle;
void preorder(struct PDS_RepoInfo* repo_handle, struct BST_Node *root);
struct PDS_RepoInfo* get_repo_handle_by_reference(char *entity_name);

struct PDS_RepoInfo* get_repo_handle_by_reference(char *entity_name)
{

	struct PDS_RepoInfo* repo_handle;
	for(int i=0;i<db_handle.db_info.num_entities;i++)
	{
		if(!strcmp(db_handle.entity_info[i].entity_name,entity_name))
		{
			repo_handle=&(db_handle.entity_info[i]);
			break;
		}
	}
	return repo_handle;



}

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
	p=fopen(a,"wb+");
	fwrite(&db_info,sizeof(struct PDS_DBInfo),1,p);
	fclose(p);
	return PDS_SUCCESS;
}
	
// P01) pds_db_open:
// Open the database info file. The file is named with .db extension.
// Example: If db_name is "bank", then file name is "bank.db"
// This should populate the PDS_DB_Master global variable named db_handle;
// struct PDS_DB_Master{
// 	int db_status;
// 	struct PDS_DBInfo db_info; // Populate this by reading from .db file
// 	struct PDS_RepoInfo entity_info[MAX_ENTITY]; // Populate this by reading from .db file
// 	struct PDS_LinkFileInfo rel_info[MAX_RELATIONSHIPS];// Populate this by reading from .db file
// };
// struct PDS_DBInfo{
// 	char db_name[30];
// 	int num_entities;
// 	struct PDS_EntityInfo entities[MAX_ENTITY];
// 	int num_relationships;
// 	struct PDS_LinkInfo links[MAX_RELATIONSHIPS];
// };
int pds_db_open( char *db_name )
{
	strcat(db_name,".db");
	if(db_handle.db_status==PDS_DB_OPEN)
		return PDS_DB_ALREADY_OPEN;
	FILE *p=fopen(db_name,"rb+");
	db_handle.db_status=PDS_DB_OPEN;
	struct PDS_DBInfo db_info;
	fread(&db_info,sizeof(struct PDS_DBInfo),1,p);
	fclose(p);
	db_handle.db_info=db_info;
	
	struct PDS_RepoInfo entity;
	for(int i=0;i<db_info.num_entities;i++)
	{
		strcpy(entity.entity_name,db_info.entities[i].entity_name);
		db_handle.entity_info[i]=entity;
		int k=pds_open(db_handle.entity_info[i].entity_name,db_info.entities[i].entity_size);
		// printf("%d\n",db_handle.entity_info[i].repo_status);

		
	}
	for(int i=0;i<db_info.num_relationships;i++)
	{
		strcpy(db_handle.rel_info[i].link_name,db_info.links[i].link_name);
		char a[30];
		strcpy(a,db_info.links[i].link_name);
		strcat(a,".lnk");
		db_handle.rel_info[i].pds_link_fp=(FILE *)fopen(a,"rb+");
		db_handle.rel_info[i].link_status=PDS_LINK_OPEN;
			
	}


	return PDS_SUCCESS;

}

// P02) pds_open
// Open the data file and index file in rb+ mode
// Update the fields of PDS_RepoInfo appropriately
// Build BST and store in pds_bst by reading index entries from the index file
// Close only the index file
int pds_open( char *repo_name, int rec_size )
{
	struct PDS_RepoInfo* repo_handle;
	repo_handle=get_repo_handle_by_reference(repo_name);
	// printf("%s %d\n",repo_handle.entity_name,rec_size);

	if(repo_handle->repo_status==PDS_ENTITY_OPEN)
		return PDS_ENTITY_ALREADY_OPEN;
	strcpy(repo_handle->entity_name,repo_name);
	repo_handle->entity_size=rec_size;
	repo_handle->repo_status=PDS_ENTITY_OPEN;
	char a1[30];
	strcpy(a1,repo_name);
	strcat(a1,".dat");
	char a2[30];
	strcpy(a2,repo_name);
	strcat(a2,".ndx");
	repo_handle->pds_data_fp=(FILE*)fopen(a1,"rb+");
	if( repo_handle->pds_data_fp == NULL ) return 1;
	repo_handle->pds_ndx_fp=(FILE*)fopen(a2,"rb+");
	if( repo_handle->pds_ndx_fp == NULL ) return 1;
	fseek( repo_handle->pds_ndx_fp, 0, SEEK_SET);
	repo_handle->pds_bst = NULL;
    bst_destroy( repo_handle->pds_bst );
    for(int i=0;i<100;i++)
    	repo_handle->free_list[i]=-1;
	fread(repo_handle->free_list,sizeof(int),100,repo_handle->pds_ndx_fp);
	struct PDS_NdxInfo *index=(struct PDS_NdxInfo *) malloc( sizeof( struct PDS_NdxInfo ) );
    fread( index, sizeof( struct PDS_NdxInfo ),1,repo_handle->pds_ndx_fp);
	bst_add_node( &repo_handle->pds_bst, index->key, index);
	while( !feof( repo_handle->pds_ndx_fp ) )
    {
        pds_load_ndx(repo_handle);
    }	
    fclose(repo_handle->pds_ndx_fp);
    return PDS_SUCCESS;
}

// P03) pds_load_ndx
// Internal function used by pds_open to read index entries into BST
int pds_load_ndx( struct PDS_RepoInfo *repo_handle )
{
	int size = sizeof( struct PDS_NdxInfo );
    struct PDS_NdxInfo *index = ( struct PDS_NdxInfo *) malloc(sizeof(size));
    fseek( repo_handle->pds_ndx_fp, size, SEEK_CUR);
    fread( index, size,1, repo_handle->pds_ndx_fp);
    if( bst_search( repo_handle->pds_bst,index->key) == NULL )
        bst_add_node( &repo_handle->pds_bst,index->key,index);
}

// P04) put_rec_by_key
// Retrieve the repo_handle from PDS_DB_Master corresponding to given entity_name
// Create an index entry with the current data file location using ftell
// Add index entry to BST using offset returned by ftell
// Check if there are any deleted location available in the free list
// If available, seek to the free location, other seek to the end of the file
// 1. Write the key at the current data file location
// 2. Write the record after writing the key
int put_rec_by_key( char *entity_name, int key, void *rec )
{
	struct PDS_RepoInfo* repo_handle;
	repo_handle=get_repo_handle_by_reference(entity_name);
	struct PDS_NdxInfo *index=(struct PDS_NdxInfo*)malloc(sizeof(struct PDS_NdxInfo));
	fseek(repo_handle->pds_data_fp,0,SEEK_END);
	int status=0;
	index->key=key;
	index->offset=ftell(repo_handle->pds_data_fp);
	struct BST_Node *node = bst_search(repo_handle->pds_bst,key );
	if(node==NULL)
	{
		int loc=-1;
		for(int i=0;i<100;i++)
		{
			if(repo_handle->free_list[i]!=-1)
			{
				loc=repo_handle->free_list[i];
				repo_handle->free_list[i]=-1;
				break;
			}
		}
		if(loc!=-1)
		{
			fseek(repo_handle->pds_data_fp,loc,SEEK_SET);
		}
		fwrite(&key,sizeof(int),1,repo_handle->pds_data_fp);
		fwrite(rec, repo_handle->entity_size, 1, repo_handle->pds_data_fp );
		status = bst_add_node( &repo_handle->pds_bst, key,index);
		if( status!= BST_SUCCESS )
        	{
           
            		fseek( repo_handle->pds_data_fp, index->offset, SEEK_SET );
            		free(index);
            		status = PDS_ADD_FAILED;
        	}



	}
	else
		status=BST_DUP_KEY;
	
	return status;
}

// P05) get_rec_by_key
// Retrieve the repo_handle from PDS_DB_Master corresponding to given entity_name
// Search for index entry in BST
// Seek to the file location based on offset in index entry
// 1. Read the key at the current file location 
// 2. Read the record after reading the key
int get_rec_by_ndx_key( char *entity_name, int key, void *rec )
{
	struct PDS_RepoInfo repo_handle;
	repo_handle=*get_repo_handle_by_reference(entity_name);
	struct BST_Node* yo=bst_search(repo_handle.pds_bst,key);
	if(yo==NULL)
		return 1;
	struct PDS_NdxInfo* index=NULL;
	index= (struct PDS_NdxInfo*)yo->data;
	fseek(repo_handle.pds_data_fp,index->offset,SEEK_SET);
	fread(&key,sizeof(int),1,repo_handle.pds_data_fp);
	fread(rec,repo_handle.entity_size,1,repo_handle.pds_data_fp);
	return PDS_SUCCESS;
}

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
{
	struct PDS_RepoInfo repo_handle;
	repo_handle=*get_repo_handle_by_reference(entity_name);
	fseek(repo_handle.pds_data_fp,0,SEEK_SET);
	int t=0;
	int b=0;
	int a;
	int offset;
	while(!feof(repo_handle.pds_ndx_fp))
	{
		
		offset=ftell(repo_handle.pds_data_fp);
		fread(&a,sizeof(int),1,repo_handle.pds_data_fp);
		fread(rec,repo_handle.entity_size,1,repo_handle.pds_data_fp);
		int c=1;
		for(int i=0;i<100;i++)
		{
			if(repo_handle.free_list[i]==offset)
				c=0;
		}
		if(c)
		{

			t++;
			if(matcher(rec,key)==0)
			{
				b=1;
				break;
				
			}	
		}
				
	}
	*io_count=t;
	if(b)
	{
		
		return PDS_SUCCESS ;
	}
	return 1;
}

// P07) update
// Retrieve the repo_handle from PDS_DB_Master corresponding to given entity_name
// Search for index entry in BST
// Seek to the file location based on offset in index entry
// Overwrite the existing record with the given record
// In case of any error, return PDS_MODIFY_FAILED
int update_by_key( char *entity_name, int key, void *newrec )
{
	struct PDS_RepoInfo repo_handle;
	repo_handle=*get_repo_handle_by_reference(entity_name);
	struct BST_Node* yo=bst_search(repo_handle.pds_bst,key);
	if(yo==NULL)
		return PDS_REC_NOT_FOUND;
	struct PDS_NdxInfo* index=NULL;
	index= (struct PDS_NdxInfo*)yo->data;
	fseek(repo_handle.pds_data_fp,index->offset,SEEK_SET);
	fwrite(&key,sizeof(int),1,repo_handle.pds_data_fp);
	int a=fwrite(newrec,repo_handle.entity_size,1,repo_handle.pds_data_fp);
	if(a<0)
	{
		return PDS_MODIFY_FAILED;

	}
	return PDS_SUCCESS;
}

// P08) pds_delete
// Retrieve the repo_handle from PDS_DB_Master corresponding to given entity_name
// Search for index entry in BST
// store the offset value in to free list
int delete_by_key( char *entity_name, int key )
{
	struct PDS_RepoInfo* repo_handle;
	repo_handle=get_repo_handle_by_reference(entity_name);
	struct BST_Node* yo=bst_search(repo_handle->pds_bst,key);
	if(yo==NULL)
		return PDS_DELETE_FAILED;
	int g=0;
	struct PDS_Link* link=(struct PDS_Link*)malloc(sizeof(struct PDS_Link));
	for(int i=0;i<db_handle.db_info.num_relationships;i++)
	{
		fseek(db_handle.rel_info[i].pds_link_fp,0,SEEK_SET);
		while(!feof(db_handle.rel_info[i].pds_link_fp))
		{
			fread(link,sizeof(struct PDS_Link),1,db_handle.rel_info[i].pds_link_fp);
			if(link->key==key)
			{
				g=1;
				break;
			}

		}
	
	}
	if(g==1)
		return PDS_LINK_FAILED;
	struct PDS_NdxInfo* index=NULL;
	index=(struct PDS_NdxInfo*)yo->data;
	int p=bst_del_node(&repo_handle->pds_bst,key);
	int k=0;
	for(int i=0;i<100;i++)
	{
		if(repo_handle->free_list[i]==-1)
		{
			repo_handle->free_list[i]=index->offset;
			k=1;
			break;
		}
	}
	if(k)
		return PDS_SUCCESS;
	else
		return PDS_DELETE_FAILED;
}

// P09) link_objects
// Retrieve the link_info from PDS_DB_Master corresponding to given link_name
// Adds a link entry to the link file establishing link between two given keys
// Example: link_data("student_course", 12, 15);
// Give error key1 does not exist or key2 does not exist or <key, linke_key> combination already exists
struct PDS_LinkFileInfo* get_link_data_by_reference(char* link_name);
struct PDS_LinkFileInfo* get_link_data_by_reference(char* link_name)
{
	struct PDS_LinkFileInfo* link_data;
	for(int i=0;i<db_handle.db_info.num_relationships;i++)
	{
		if(!strcmp(db_handle.rel_info[i].link_name,link_name))
		{
			link_data=&(db_handle.rel_info[i]);
			break;
		}
	}
	return link_data;


}
int link_data(char *link_name, int key, int linked_key)
{
	
	struct PDS_LinkFileInfo *link_data=get_link_data_by_reference(link_name);
	if(link_data==NULL)
		return PDS_LINK_FAILED;
	char entity1[30],entity2[30];
	for(int i=0;i<db_handle.db_info.num_relationships;i++)
	{

		if(!strcmp(db_handle.db_info.links[i].link_name,link_name))
		{
			strcpy(entity1,db_handle.db_info.links[i].pds_name1);
			strcpy(entity2,db_handle.db_info.links[i].pds_name2);
			break;

		}

	}
	struct PDS_RepoInfo *repo_handle1;
	struct PDS_RepoInfo *repo_handle2;
	repo_handle1=get_repo_handle_by_reference(entity1);
	repo_handle2=get_repo_handle_by_reference(entity2);
	struct BST_Node *node1=bst_search(repo_handle1->pds_bst,key);
	struct BST_Node *node2=bst_search(repo_handle2->pds_bst,linked_key);
	if(node1==NULL)
	{
		fprintf(stderr,"Key1 is missing\n");
		return PDS_LINK_FAILED;
	}
	if(node2==NULL)
	{
		fprintf(stderr,"Key2 is missing\n");
		return PDS_LINK_FAILED;
	}

	fseek(link_data->pds_link_fp,0,SEEK_SET);
	struct PDS_Link* link=(struct PDS_Link*)malloc(sizeof(struct PDS_Link));
	while(!feof(link_data->pds_link_fp))
	{
		fread(link,sizeof(struct PDS_Link),1,link_data->pds_link_fp);
		if(link->key==key && link->linked_key==linked_key)
		{
			fprintf(stderr,"Link already exists\n");
			return PDS_LINK_FAILED;
							
		}
	}
	link->key=key;
	link->linked_key=linked_key;
	fwrite(link,sizeof(struct PDS_Link),1,link_data->pds_link_fp);
	return PDS_SUCCESS;

	

}

// P10) getLinkedData
// Returns the linked data for a given key
// Example: Retrieve all courses (linked_data) done by a given student (data_key)
int get_linked_data( char *link_name, int data_key, struct PDS_LinkedKeySet *linked_data )
{
	struct PDS_LinkFileInfo *link_data=get_link_data_by_reference(link_name);
	
		linked_data->key=data_key;
		
		fseek(link_data->pds_link_fp,0,SEEK_SET);
		struct PDS_Link* link=(struct PDS_Link*)malloc(sizeof(struct PDS_Link));
		int g=0;
		int lastkey;
		while(!feof(link_data->pds_link_fp))
		{
			fread(link,sizeof(struct PDS_Link),1,link_data->pds_link_fp);
			if(link->key==data_key)
			{
				linked_data->linked_keys[g]=link->linked_key;

				g++;

			}
			lastkey=link->key;
			// printf("%d %d\n",link->key,link->linked_key);

		}
		if(data_key==lastkey)
			g-=1;
		linked_data->link_count=g;
		// printf("%d\n",g);
		return PDS_SUCCESS;


	



}

// P11) pds_close
// Retrieve the repo_handle from PDS_DB_Master corresponding to given entity_name
// Open the index file in wb mode (write mode, not append mode)
// Unload the BST into the index file by traversing it in PRE-ORDER (overwrite the entire index file)
// Free the BST by call bst_destroy()
// Close the index file and data file
void preorder(struct PDS_RepoInfo* repo_handle,struct BST_Node* root)
{
	if(root==NULL)
		return;
	struct PDS_NdxInfo *index = (struct PDS_NdxInfo *) (root->data);
	fwrite(index,sizeof(struct PDS_NdxInfo ),1,repo_handle->pds_ndx_fp );
	fseek( repo_handle->pds_ndx_fp, sizeof(struct PDS_NdxInfo ),SEEK_CUR);
	preorder( repo_handle,root->left_child );
    preorder( repo_handle,root->right_child );


}
int pds_close( char *entity_name )
{
	struct PDS_RepoInfo* repo_handle;
	repo_handle=get_repo_handle_by_reference(entity_name);
	strcat(repo_handle->entity_name,".ndx");
	repo_handle->pds_ndx_fp=(FILE*)fopen(repo_handle->entity_name,"w");
	fseek(repo_handle->pds_ndx_fp,0,SEEK_SET);
	fwrite(repo_handle->free_list,sizeof(int),100,repo_handle->pds_ndx_fp);
	preorder(repo_handle,repo_handle->pds_bst);	
	bst_destroy(repo_handle->pds_bst);
	repo_handle->pds_bst=NULL;
	strcpy(repo_handle->entity_name,"");
	fclose(repo_handle->pds_ndx_fp);
	fclose(repo_handle->pds_data_fp);
	repo_handle->repo_status=PDS_ENTITY_CLOSED;
	return PDS_SUCCESS;

}

// P12) pds_db_close
// Close all the entity files by calling pds_close
// Close all link files
int pds_db_close()
{
	int k=0;
	for(int i=0;i<db_handle.db_info.num_entities;i++)
	{
		k=k|pds_close(db_handle.entity_info[i].entity_name);
	}
	for(int i=0;i<db_handle.db_info.num_relationships;i++)
	{
		
		
		db_handle.rel_info[i].link_status=PDS_LINK_CLOSED;
		
		fseek(db_handle.rel_info[i].pds_link_fp,0,SEEK_SET);
		struct PDS_LinkFileInfo *link_data=get_link_data_by_reference(db_handle.rel_info[i].link_name);
		strcpy(db_handle.rel_info[i].link_name,"");
		struct PDS_Link* link=(struct PDS_Link*)malloc(sizeof(struct PDS_Link));
		
		fclose(db_handle.rel_info[i].pds_link_fp);
	}
	db_handle.db_status=PDS_DB_CLOSED;
	if(k)
		return 1;
	return PDS_SUCCESS;


}