#include<stdio.h>
#include<stdlib.h>
#include<errno.h>
#include"pds.h"

int main(int argc,char *argv[])
{

	char *db_name;
	char *schema_file_name;
	if(argc!=3)
	{
		fprintf(stderr, "Usage: %s <repo-name> <dump-file-name>\n", argv[0]);
		exit(1);
	}
	db_name=argv[1];
	schema_file_name=argv[2];
	int status=pds_create_schema(schema_file_name);
	if(status!=PDS_SUCCESS)
	{

		fprintf(stderr,"Unable to create schema from %s - Exit Status %d\n",schema_file_name,status);
		exit(status);
	}


}