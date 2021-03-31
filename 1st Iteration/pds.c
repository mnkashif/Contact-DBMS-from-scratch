#include <stdlib.h>
#include <stdio.h>
#include<string.h>
#include"pds.h"
#include"bst.h"
struct PDS_RepoInfo repo_handle;
int pds_open( char *repo_name, int rec_size )
{
	if(repo_handle.repo_status==PDS_REPO_OPEN)
		return PDS_REPO_ALREADY_OPEN;
	strcpy(repo_handle.pds_name,repo_name);
	repo_handle.rec_size=rec_size;
	repo_handle.repo_status=PDS_REPO_OPEN;
	strcat(repo_name,".dat");
	repo_handle.pds_data_fp=(FILE*)fopen(repo_name,"wb+");
	repo_handle.pds_bst=NULL;
	if(repo_handle.pds_data_fp!=NULL)
		return 0;
	return 1;

}

int put_rec_by_key( int key, void *rec )
{
	struct PDS_NdxInfo *index=(struct PDS_NdxInfo*)malloc(sizeof(struct PDS_NdxInfo));
	fseek(repo_handle.pds_data_fp,0,SEEK_END);
	int status;
	index->key=key;
	index->offset=ftell(repo_handle.pds_data_fp);
	status=bst_add_node(&repo_handle.pds_bst,key,index);
	fwrite(rec,repo_handle.rec_size,1,repo_handle.pds_data_fp);
	free(index);
	return status!=BST_SUCCESS?PDS_ADD_FAILED:PDS_SUCCESS;


}

int get_rec_by_key( int key, void *rec )
{

	struct BST_Node* yo=bst_search(repo_handle.pds_bst,key);;
	if(yo==NULL)
		return PDS_REC_NOT_FOUND;
	struct PDS_NdxInfo *index=(struct PDS_NdxInfo*)malloc(sizeof(struct PDS_NdxInfo));
	index=yo->data;
	fseek(repo_handle.pds_data_fp,index->offset,SEEK_SET);
	fread(rec,repo_handle.rec_size,1,repo_handle.pds_data_fp);
	return PDS_SUCCESS;


}
int pds_close()
{

	fclose(repo_handle.pds_data_fp);
	bst_free(repo_handle.pds_bst);
	repo_handle.repo_status=PDS_REPO_CLOSED;
	return PDS_SUCCESS;

}
