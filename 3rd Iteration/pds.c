#include <stdlib.h>
#include <stdio.h>
#include<string.h>
#include"pds.h"
#include"bst.h"
#include<errno.h>
struct PDS_RepoInfo repo_handle;
void preorder( struct BST_Node *root);
int pds_load_ndx()
{
	int size = sizeof( struct PDS_NdxInfo );
    struct PDS_NdxInfo *index = ( struct PDS_NdxInfo *) malloc(sizeof(size));
   
    fseek( repo_handle.pds_ndx_fp, size, SEEK_CUR);
    fread( index, size,1, repo_handle.pds_ndx_fp);
    if( bst_search( repo_handle.pds_bst,index->key) == NULL )
        bst_add_node( &repo_handle.pds_bst,index->key,index);
    
    



}
int pds_open( char *repo_name, int rec_size )
{
	if(repo_handle.repo_status==PDS_REPO_OPEN)
		return PDS_REPO_ALREADY_OPEN;
	strcpy(repo_handle.pds_name,repo_name);
	repo_handle.rec_size=rec_size;
	repo_handle.repo_status=PDS_REPO_OPEN;
	char a1[30];
	strcpy(a1,repo_name);
	strcat(a1,".dat");
	char a2[30];
	strcpy(a2,repo_name);
	strcat(a2,".ndx");
	repo_handle.pds_data_fp=(FILE*)fopen(a1,"rb+");
	if( repo_handle.pds_data_fp == NULL ) return 1;
	repo_handle.pds_ndx_fp=(FILE*)fopen(a2,"rb+");
	if( repo_handle.pds_ndx_fp == NULL ) return 1;
	fseek( repo_handle.pds_ndx_fp, 0, SEEK_SET);
	repo_handle.pds_bst = NULL;
    bst_destroy( repo_handle.pds_bst );
    struct PDS_NdxInfo *index=(struct PDS_NdxInfo *) malloc( sizeof( struct PDS_NdxInfo ) );
    fread( index, sizeof( struct PDS_NdxInfo ),1,repo_handle.pds_ndx_fp);
	bst_add_node( &repo_handle.pds_bst, index->key, index);
	while( !feof( repo_handle.pds_ndx_fp ) )
    {
        pds_load_ndx();
    }	
    fclose(repo_handle.pds_ndx_fp);
    return PDS_SUCCESS;
	

}


int put_rec_by_key( int key, void *rec )
{

	struct PDS_NdxInfo *index=(struct PDS_NdxInfo*)malloc(sizeof(struct PDS_NdxInfo));
	fseek(repo_handle.pds_data_fp,0,SEEK_END);
	int status=0;
	index->key=key;
	index->offset=ftell(repo_handle.pds_data_fp);
	struct BST_Node *node = bst_search(repo_handle.pds_bst,key );
	if(node==NULL)
	{
		fwrite( rec, repo_handle.rec_size, 1, repo_handle.pds_data_fp );
		status = bst_add_node( &repo_handle.pds_bst, key,index);
		if( status!= BST_SUCCESS )
        {
           
            fseek( repo_handle.pds_data_fp, index->offset, SEEK_SET );
            free(index);
            status = PDS_ADD_FAILED;
        }



	}
	else
		status=BST_DUP_KEY;
	
	return status;

}

int get_rec_by_key( int key, void *rec )
{

	struct BST_Node* yo=bst_search(repo_handle.pds_bst,key);;
	if(yo==NULL)
		return PDS_REC_NOT_FOUND;
	struct PDS_NdxInfo* index=NULL;
	index= (struct PDS_NdxInfo*)yo->data;
	fseek(repo_handle.pds_data_fp,index->offset,SEEK_SET);
	// fread(rec,sizeof(struct PDS_NdxInfo),1,repo_handle.pds_data_fp);
	fread(rec,repo_handle.rec_size,1,repo_handle.pds_data_fp);
	return PDS_SUCCESS;


}
int pds_close()
{
	strcat(repo_handle.pds_name,".ndx");
	
	repo_handle.pds_ndx_fp=(FILE*)fopen(repo_handle.pds_name,"wb+");
	preorder( repo_handle.pds_bst );	

	bst_destroy(repo_handle.pds_bst);
	repo_handle.pds_bst=NULL;
	strcpy(repo_handle.pds_name,"");
	fclose(repo_handle.pds_ndx_fp);
	fclose(repo_handle.pds_data_fp);
	repo_handle.repo_status=PDS_REPO_CLOSED;
	return PDS_SUCCESS;


}
void preorder(struct BST_Node* root)
{
	if(root==NULL)
		return;
	struct PDS_NdxInfo *index = (struct PDS_NdxInfo *) (root->data);
	fwrite(index,sizeof(struct PDS_NdxInfo ),1,repo_handle.pds_ndx_fp );
	fseek( repo_handle.pds_ndx_fp, sizeof(struct PDS_NdxInfo ),SEEK_CUR);
	preorder( root->left_child );
    preorder( root->right_child );


}
