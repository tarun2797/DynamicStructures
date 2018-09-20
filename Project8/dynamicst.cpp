# define _CRT_SECURE_NO_WARNINGS
# define  size_of_chunk_bit 35
#include<stdio.h>
#include<malloc.h>
#include<stdlib.h>
#include<string.h>
/* METADATA sructure to store information about schemas */

/*
attributes -> stores columns in schema
no_of_attributes -> number of columns in a schema
count -> Total number of values stored in schema
mark -> If 1-indicates int and 0 indicates a string
primary_key -> stores index of primary key
size_of_record -> stores bytes needed to store one record of schema
chunk -> Total memory needed to store data of schema in terms of characters
*/
struct metadata
{
	char attributes[15][10];
	int no_of_attributes,count;
	int mark[20];
	int primary_key;
	int size_of_record;
	char *chunk;
};
/* schema operator functions */
/*
- it is for parsing the command for import
- it gets filename and schema number
*/
void parse_import_comand(char *s, char *file, int *schema)
{
	int len = 0, i;
	char *str = (char *)malloc(sizeof(char) * 10);
	for (i = 0; s[i] != ' '; i++)
		str[i] = s[i];
	str[i] = '\0';
	if (strcmp(str, "import"))
	{
		printf("invalid command\n");
	}
	while (s[i] == ' ')
		i++;
	for (; s[i] != ' '; i++)
	{
		file[len++] = s[i];
	}
	file[len] = '\0';
	while (s[i] == ' ')
		i++;
	*schema = s[i] - 48;
}
/*
-it is for parsing the command for join
- it gets the numbers of the two schemas to be joined
*/
void parse_join_comand(char *s, int *s1, int *s2)
{
	int i;
	char *str = (char *)malloc(sizeof(char) * 10);
	for (i = 0; s[i] != ' '; i++)
		str[i] = s[i];
	str[i] = '\0';
	if (strcmp(str, "join"))
	{
		printf("invalid command\n");
	}
	while (s[i] == ' ')
		i++;
	*s1 = s[i] - 48;
	i++;
	while (s[i] == ' ')
		i++;
	*s2 = s[i] - 48;
	//printf("join - %d,%d\n", *s1, *s2);
}
/*
- it is for parsing the command for flush
- it gets filename and schema number
*/
void parse_flush_comand(char *s, char *file, int *schema)
{
	int len = 0, i;
	char *str = (char *)malloc(sizeof(char) * 10);
	for (i = 0; s[i] != ' '; i++)
		str[i] = s[i];
	str[i] = '\0';
	if (strcmp(str, "flush"))
	{
		printf("invalid command\n");
	}
	while (s[i] == ' ')
		i++;
	for (; s[i] != ' '; i++)
	{
		file[len++] = s[i];
	}
	file[len] = '\0';
	while (s[i] == ' ')
		i++;
	*schema = s[i] - 48;
}
/*
- function to copy value of string y to x
*/
void strcpy(char *x, char *y)
{
	int i;
	for (i = 0; y[i] != '\0'; i++)

		x[i] = y[i];

	x[i] = '\0';

	return;
}
/*
- function for creating schema
- it takes comand from console,parses it and stores data into metadata structure
*/
void create_schema(char *s,metadata **md,int sn)
{
	int i,len = 0,ind = 0;
	for (i = 0; s[i] != ' '; i++) {}
	for (i++; s[i] != ' '; i++) {}
	for (i++; i<strlen(s); i++)
	{
		if (s[i] == ':')
		{
			md[sn]->attributes[ind][len++] = '\0';
			if (s[i + 1] == 'i')
				md[sn]->mark[ind] = 1;
			else
				md[sn]->mark[ind] = 0;
			ind++;
			len = 0;
			while (s[i]&&s[i]!=',')
			{
				i++;
			}
		}
		else
		{
			md[sn]->attributes[ind][len++] = s[i];
		}
	}
	md[sn]->no_of_attributes = ind;
	md[sn]->count = 0;
	md[sn]->primary_key = 0;
	md[sn]->size_of_record = md[sn]->no_of_attributes * size_of_chunk_bit;
	/*for (int i = 0; i < ind; i++)
		printf("%s-%d\n", md[sn]->attributes[i],md[sn]->mark[i]);
	printf("%d\n", md[sn]->no_of_attributes);*/
}
/*
- function is used for printing the each schema details
*/
void console_printing(int schema,metadata **md)
{
	//printf("entered\n");
	int ind = 0;
	int no_of_records = md[schema]->count / md[schema]->no_of_attributes;
	//printf("number = %d\n", no_of_records);
	for (int i = 0; i < no_of_records; i++)
	{
		for (int j = 0; j < md[schema]->no_of_attributes; j++, ind += size_of_chunk_bit)
		{
			printf("%s",md[schema]->chunk+ind);
			if (j != md[schema]->no_of_attributes - 1)
				printf(",");
		}
		printf("\n");
	}
	//printf("exited\n");
}
/*
- function for importing a file of specified schema
- it reads values from files and stores it into the schema chunks
*/
void import_helper(char *filename,int schema,metadata **md)
{
	char *s = (char *)malloc(sizeof(char)*80);
	FILE *fp;
	fp = fopen(filename, "r");      // printf("%s-%d\n", filename,fp);
	char c = 't';
	int len, j, ind = md[schema]->count*size_of_chunk_bit;
	while (1)
	{
		for (int i = 0; i < md[schema]->no_of_attributes; i++)
		{
			if (i != md[schema]->no_of_attributes - 1)
			{
				fscanf(fp, "%[^,]s", s);
				c = fgetc(fp);
			}
			else
			{
				fscanf(fp, "%s", s);
				c = fgetc(fp);
			}
			//printf("%s,",s);
			for (j = 0; s[j]; j++)
			{
				md[schema]->chunk[ind+j] = s[j];
			}
			md[schema]->chunk[ind+j] = '\0';
			md[schema]->count += 1;
			//printf("%d\n", ind);
			ind = md[schema]->count*size_of_chunk_bit;
		}
		//printf("\n");
		if (c == EOF)
			break;
	}
	fclose(fp);
	//printf("ended\n");
}
/*
- function that takes character string 's' attribute as an argument
- returns a boolean value whether it is primary key or not
- it is used for joining schemas to make sure that primary key is not copied into the joined schema twice
*/
int is_primary_key(char *s,int schema,metadata **md)
{
	for (int i = 0; i < md[schema]->no_of_attributes; i++)
	{
		if (strcmp(s,md[schema]->attributes[i]) == 0)
			return 1;
	}
	return 0;
}
/*
- it creates joined schema 'sn' of two different schemas s1 and s2
*/
void create_join_schema(int s1,int s2,metadata **md,int sn)
{
	int ind = 0;
	for (int i = 0; i < md[s1]->no_of_attributes; i++)
	{
		strcpy(md[sn]->attributes[ind],md[s1]->attributes[i]);
		md[sn]->mark[ind] = md[s1]->mark[i];
		ind++;
	}
	for (int i = 0; i < md[s2]->no_of_attributes; i++)
	{
		if (!is_primary_key(md[s2]->attributes[i],s1,md))
		{
			strcpy(md[sn]->attributes[ind], md[s2]->attributes[i]);
			md[sn]->mark[ind] = md[s2]->mark[i];
			ind++;
		}
	}
	md[sn]->no_of_attributes = ind;
	md[sn]->count = 0;
	md[sn]->primary_key = 0;
	md[sn]->size_of_record = md[sn]->no_of_attributes * size_of_chunk_bit;
	/*for (int i = 0; i < ind; i++)
		printf("%s-%d\n", md[sn]->attributes[i], md[sn]->mark[i]);
	printf("%d\n", md[sn]->no_of_attributes);*/
}
/*
- it gives index of corresponding record of other schema while joining two schemas
- pat1 and pat2 are patterns followed by the schemas while storing values in the character arrays
*/
int get_index_of_corresponding_record_in_schema2(int key,int pat1,int pat2,metadata **md,int s1,int s2)
{
	char *s = (char *)malloc(sizeof(char)*40);
	strcpy(s,md[s1]->chunk+(key*pat1));
	//printf("get index - %s\n",s);
	int no_of_records = md[s2]->count * size_of_chunk_bit;
	for (int i = 0; i < no_of_records; i += pat2)
	{
		if (strcmp(s,md[s2]->chunk+i) == 0)
		{
			//printf("got for %s,%s\n", s, md[s2]->chunk + i);
			return i+size_of_chunk_bit;
		}
	}
	return -1;
}
/*
- this function copies values into joined schema from schemas s1 and s2 after creating a metastructure for joined schema
- it takes help of get_index_of_corresponding_record_in_schema2() to join them correctly based on primary key
*/
void join_helper1(int s1,int s2,int sn,metadata **md)
{
	//printf("t--entered\n");
	int ind1 = 0,ind2 = 0,ind = 0;   // iterators for schema1,schema2 and join schema
	int no_of_records = md[s1]->count / md[s1]->no_of_attributes;
	int n1 = md[s1]->no_of_attributes*size_of_chunk_bit, n2 = md[s2]->no_of_attributes*size_of_chunk_bit;          // corresponding chunck sizes
	for (int z = 0; z<no_of_records ; z++)
	{
		for (int i = 0; i < md[s1]->no_of_attributes; i++)
		{
			strcpy(md[sn]->chunk + ind, md[s1]->chunk + ind1);
			//printf("%s-",md[s1]->chunk + ind1);
			//printf("%s,", md[sn]->chunk + ind);
			ind += size_of_chunk_bit;
			ind1 += size_of_chunk_bit;
			md[sn]->count += 1;
		}
		int ind2 = get_index_of_corresponding_record_in_schema2(ind1/n1-1,n1,n2,md,s1,s2);
		//printf("\n%d\n", ind2);
		if (ind2 != -1)
		{
			for (int i = 1; i < md[s2]->no_of_attributes; i++)
			{
				strcpy(md[sn]->chunk + ind, md[s2]->chunk + ind2);
				//printf("%s\n", md[s2]->chunk + ind2);
				//printf("%s,", md[sn]->chunk + ind);
				ind += size_of_chunk_bit;
				ind2 += size_of_chunk_bit;
				md[sn]->count += 1;
			}
		}
		else
		{
			for (int i = 1; i < md[s2]->no_of_attributes; i++)
			{
				strcpy(md[sn]->chunk + ind,"NULL");
				ind += size_of_chunk_bit;
				md[sn]->count += 1;
			}
		}
		//printf("\n");
	}
	//printf("t--exited\n");
}
/*
- the two join helper funcions are for outer joining of schemas so that loss of data is avoided
- Duplication of data is also handles by these functions
*/
void join_helper2(int s1, int s2, int sn, metadata **md)
{
	int ind = md[sn]->count * size_of_chunk_bit;
	int ind1 = 0;
	int no_of_records = md[s1]->count / md[s1]->no_of_attributes;
	int n1 = md[s1]->no_of_attributes * size_of_chunk_bit, n2 = md[s2]->no_of_attributes * size_of_chunk_bit;          // corresponding chunck sizes
	for (int z = 0; z < no_of_records; z++)
	{
		int ind2 = get_index_of_corresponding_record_in_schema2(z, n1, n2, md, s1, s2);
		if (ind2 == -1)
		{
			//printf("for %s at %d\n", md[s1]->chunk + ind1,ind2);
			for (int i = 0; i < md[s2]->no_of_attributes; i++)
			{
				if (i!=0)
					strcpy(md[sn]->chunk + ind, "NULL");
				else
				{
					strcpy(md[sn]->chunk + ind, md[s1]->chunk + ind1);
					ind1 += size_of_chunk_bit;
				}
				ind += size_of_chunk_bit;
				md[sn]->count += 1;
			}
			for (int i = 1; i < md[s1]->no_of_attributes; i++)
			{
				strcpy(md[sn]->chunk + ind, md[s1]->chunk + ind1);
				//printf("%s-",md[s1]->chunk + ind1);
				//printf("%s,", md[sn]->chunk + ind);
				ind += size_of_chunk_bit;
				ind1 += size_of_chunk_bit;
				md[sn]->count += 1;
			}
		}
	}
}
/*
- it stores the imported schemas into new files
- it simply opens the file and writes the data contained in the metadata structure
*/
void flush_helper(char *filename,int schema,metadata **md)
{
	FILE *fp;
	fp = fopen(filename,"w");
	int ind = 0;
	int no_of_records = md[schema]->count / md[schema]->no_of_attributes;
	for (int i = 0; i < no_of_records; i++)
	{
		for (int j = 0; j < md[schema]->no_of_attributes; j++,ind += size_of_chunk_bit)
		{
			fprintf(fp, "%s", md[schema]->chunk + ind);
			if (j != md[schema]->no_of_attributes - 1)
				fprintf(fp, "%c", ',');
		}
		fprintf(fp,"%c",'\n');
	}
	fclose(fp);
}

/* query handlers */
/*
- it gives the memory location of the attribute with index -'qind' over the entire memory chunck
- which belongs to the record - 'record_no' and to the schema - 'schema_no'
*/
int get_index_of_value(int record_no, int qind, metadata **md, int schema_no)
{
	int index = md[schema_no]->size_of_record*(record_no - 1);
	index += (qind * size_of_chunk_bit);
	return index;
}
/*
- it just gives index value of an attribute in the schema
- it doesn't give memory location over entire memory chunck unlike get_index_of_value()
*/
int get_index_of_attribute(char *s, int schema_no, metadata **md)
{
	for (int i = 0; i < md[schema_no]->no_of_attributes; i++)
	{
		//printf("%s,", md[schema_no]->attributes[i]);
		if (strcmp(s, md[schema_no]->attributes[i]) == 0)
			return i;
	}
	printf("\nsomrthing wrong for %s in command , please enter correctly\n",s);
}
/*
- it prints output for the query on to the console
- for both number type and string type queries
*/
void output_printer(int *indices, int len, int record_no, metadata **md, int schema_no)
{
	for (int i = 0; i < len; i++)
	{
		int ind = get_index_of_value(record_no, indices[i], md, schema_no);
		printf("%s,", md[schema_no]->chunk + ind);
	}
	printf("\n");
}
/*
- it is used for changing a number string to an integer
- it is used only for number type queries
*/
int parse_char_to_int(char *s)
{
	int n = 0;
	for (int i = 0; s[i]; i++)
	{
		n *= 10;
		n += s[i] - 48;
	}
	return n;
}
/*
- it gives the value of any attribute with index -'qind' , with schema - 'schema_no', record number-'record_no'
*/
int get_value(int record_no,int qind,metadata **md,int schema_no)
{
	int index = md[schema_no]->size_of_record*(record_no - 1);
	index += (qind*size_of_chunk_bit);
	char *s = (char *)malloc(sizeof(char)*30);
	s = md[schema_no]->chunk + index;
	return parse_char_to_int(s);
}
/*
- this function returns boolean value based on the type of operation the query has contained
- it has two arguments . one is the 'qval' - the value mentioned in query and the other 'x'- the value of all other records
*/
int num_query_helper(int qval, char op, int x)
{
	if (op == '>')
	{
		if (x > qval)
			return 1;
		return 0;
	}
	else if (op == '<')
	{
		if (x < qval)
			return 1;
		return 0;
	}
	else if (op == '=')
	{
		if (x == qval)
			return 1;
		return 0;
	}
}
/*
- it iterates over all the records of the specified schema for handling queries involving arithmetic operations i.e number queries
- and makes use  of all other number handling query functions
*/
void num_query(int *indices,int len,int qval,char op,int qind,metadata **md,int schema_no)
{
	int no_of_records = md[schema_no]->count/md[schema_no]->no_of_attributes;
	for (int i = 1; i <= no_of_records; i++)
	{
		int x = get_value(i,qind,md,schema_no);
		if (num_query_helper(qval,op,x))
		{
			output_printer(indices,len,i,md,schema_no);
		}
	}
}
/*
- it takes two strings as input and returns whether they are equal or not
- it is used for handling queries like "name=tarun"
*/
int str_match(char *s1, int l1, char *s2, int l2)
{
	if (l1 != l2)
		return 0;
	for (int i = 0; i < l1; i++)
		if (s1[i] != s2[i])
			return 0;
	return 1;
}
/*
- it takes two strings . one as pattern and other as text
- it returns true if pattern is contained in the text else it returns false
- it is used for handling queries like "name%ta"
*/
int str_contains(char* pat,int l1,char* txt,int l2)
{
	int M = strlen(pat);
	int N = strlen(txt);
	for (int i = 0; i <= N - M; i++) 
	{
		int j;
		for (j = 0; j < M; j++)
			if (txt[i + j] != pat[j])
				break;
		if (j == M)
			return 1;
	}
	return 0;
}
/*
- it is used for handling type of operation on strings whether 'string contains' or 'string match'
*/
int str_query_helper(char *qstr, char op, char *x)
{
	if (op == '=')
	{
		//printf("entered for %s,%s\n",qstr,x);
		if (str_match(qstr, strlen(qstr), x, strlen(x)))
			return 1;
		return 0;
	}
	else if (op == '%')
	{
		if (str_contains(qstr, strlen(qstr), x, strlen(x)))
			return 1;
		return 0;
	}
}
/*
- it iterates over all the records of the specified schema for handling queries involving string operations
- and makes use  of all other string handling query functions
*/
void str_query(int *indices, int len, char *qstr, char op, int qind, metadata **md, int schema_no)
{
	int no_of_records = md[schema_no]->count / md[schema_no]->no_of_attributes;
	for (int i = 1; i <= no_of_records; i++)
	{
		int index = get_index_of_value(i,qind,md,schema_no);
		char *s = md[schema_no]->chunk + index;
		if (str_query_helper(qstr, op, s))
		{
			output_printer(indices,len,i,md,schema_no);
		}
	}
}
/*
- it takes the query string and tokenises the required columns which need to be displayed
*/
void get_tokens(char s[], int *indarr, int *l, int *qval, char *op, int *schema_no, char *qcol, char *qstr, metadata **md)
{
	int i;
	*qval = 0;
	for (i = 0; s[i] != ' '; i++) {} // avoiding select
	char col[15][10], con[10];
	int ind = 0, len = 0;
	for (i++; s[i] != ' '; i++)
	{
		if (s[i] != ',')
		{                                   //printf("%c\n", s[i]);
			col[ind][len++] = s[i];
		}
		else
		{
			col[ind][len++] = '\0';
			ind++;
			len = 0;
		}
	}
	col[ind][len++] = '\0';
	ind++;
	*l = ind;
	len = 0;                //printf("\nsrings are : "); for (int i = 0; i < ind; i++) printf("%s\n", col[i]);
	for (i++; s[i] != ' '; i++) {}   // avoiding from
	*schema_no = s[++i] - 48;   //printf("schema - %d\n", *schema_no);
	for (i += 2; s[i] != ' '; i++) {}   // avoiding where   
	for (int z = 0; z < ind; z++)
	{
		indarr[z] = get_index_of_attribute(col[z], *schema_no, md);
	}
	for (i++; (s[i] >= 'a'&&s[i] <= 'z')||(s[i]>='0'&&s[i]<='9'); i++)
	{
		qcol[len++] = s[i];
	}
	qcol[len++] = '\0';    //printf("qcol - %s\n", qcol);
	*op = s[i];
	len = 0;
	for (i++; s[i]; i++)
	{
		qstr[len++] = s[i];
		*qval *= 10;
		*qval += s[i] - 48;
	}                      //printf("op,qval - %c,%d\n", *op, *qval);
	qstr[len++] = '\0';
}
int main()
{
	int ind = 0,sn=0,s1,s2;
	char ch[10];
	char * filename = (char *)malloc(sizeof(char)*10);
	int schema = 0;
	char *comand = (char *)malloc(sizeof(char)*50);
	metadata **md = (metadata **)malloc(sizeof(metadata *)*10);
	for (int i = 0; i < 10; i++)
		md[i] = (metadata *)malloc(sizeof(metadata));
	int *indices = (int *)malloc(sizeof(int) * 15);
	char *qcol = (char *)malloc(sizeof(char) * 10);
	char *qstr = (char *)malloc(sizeof(char) * 10);
	int qval, len,schema_no,qind;
	char op;
	while (1)
	{
		//printf("Enter keyword : create/import/join/flush/query/exit 0 process\n");
		scanf("%s\n", ch);
		if (!strcmp(ch,"create"))
		{
			scanf("%[^\n]s", comand);   
			/*  sample
				eg1 : "create schema roll:int,name:str,m1:int,m2:int,m3:int,m4:int"
				eg2 : "create schema roll:int,ph:str,add:str,city:str,pin:int"
			*/
			create_schema(comand, md, sn);
			sn++;
		}
		else if (!strcmp(ch, "import"))
		{
			scanf("%[^\n]s", comand);
			/*	sample
				eg1 : "import f1.txt 0"
					   here import is keyword , f1.txt is filename and 0 is schema number
				eg2 : "import f2.txt 1"
					   here import is keyword , f2.txt is filename and 1 is schema number
			*/
			parse_import_comand(comand, filename, &schema);
			if (!md[schema]->count)
				md[schema]->chunk = (char *)malloc(sizeof(char) * 25000);
			import_helper(filename, schema, md); 
			//console_printing(schema,md); 
		}
		else if (!strcmp(ch, "join"))
		{
			scanf("%[^\n]s",comand);       
			/*
			    eg : "join 0 1"
					  here join is keyword and 0 , 1 are schema numbers to be joined 
			*/ 
			parse_join_comand(comand,&s1,&s2);
			create_join_schema(s1, s2, md, sn);
			if (!md[sn]->count)
				md[sn]->chunk = (char *)malloc(sizeof(char) * 25000);
			join_helper1(s1, s2, sn, md);    
			join_helper2(s2, s1, sn, md);
			//console_printing(sn, md);
			sn++;
		}
		else if (!strcmp(ch, "flush"))
		{
			scanf("%[^\n]s", comand);
			/*	sample
			eg1 : "flush fl1.txt 0"
					here flush is keyword , f1.txt is filename and 0 is schema number
			eg2 : "flush fl2.txt 1"
					here flush is keyword , f2.txt is filename and 1 is schema number
			eg3 : "flush fl3.txt 2"
					here flush is keyword , f2.txt is filename and 1 is schema number
			*/
			parse_flush_comand(comand, filename, &schema);
			flush_helper(filename, schema, md);
		}
		else if (!strcmp(ch, "query"))
		{
			scanf("%[^\n]s", comand);
			/*
				It supports 6 types of queries

				eg 1 : "select roll,name from 0 where m1>90"   

				eg 2 : "select roll,add,pin from 1 where roll<60"
					
				eg 3 : "select name,ph,add from 2 where m2=45"

				The above 3 types of queries are number queries

				eg 4 : "select roll,name,ph from 2 where name=Tiger" 

				eg 5 : "select roll,add from 1 where ph=9402753835"

				eg 6 : "select roll,name,ph from 2 where name%o" 

				The above 3 types of queries are string queries
			*/
			get_tokens(comand,indices, &len, &qval, &op, &schema_no,qcol, qstr,md);
			qind = get_index_of_attribute(qcol,schema_no,md);
			if (md[schema_no]->mark[qind])
			{
				num_query(indices, len, qval, op, qind,md,schema_no);
			}
			else
			{
				str_query(indices, len, qstr, op, qind, md,schema_no);
			}
		}
		else if (!strcmp(ch,"exit"))
		{
			break;
		}
		else
		{
			printf("Invalid command\n");
		}
	}
	system("pause");
	return 0;
}
/*
Sample Input 1 for performing queries

create
create schema roll:int,name:str,m1:int,m2:int,m3:int,m4:int
import
import f1.txt 0
create
create schema roll:int,ph:str,add:str,city:str,pin:int
import
import f2.txt 1
join
join 0 1
flush 
flush fl1.txt 0
flush
flush fl2.txt 1
flush
flush fl3.txt 2
query
select roll,add from 1 where ph=94027538size_of_chunk_bit
*/
/*
Sample Input 2 for performing multiple imports and flushing
create
create schema roll:int,name:str,m1:int,m2:int,m3:int,m4:int
import
import s11.txt 0
import
import s12.txt 0
import
import s13.txt 0
flush 
flush fl1.txt 0
create
create schema roll:int,ph:str,add:str,city:str,pin:int
import
import s21.txt 1
import
import s22.txt 1
import
import s23.txt 1
flush
flush fl2.txt 1
join
join 0 1
*/
/*
Sample Input 3 for Joining of schemas having unmatched records
create
create schema roll:int,name:str,m1:int,m2:int,m3:int,m4:int
import
import s11.txt 0
create
create schema roll:int,ph:str,add:str,city:str,pin:int
import
import s21.txt 1
join
join 0 1
*/