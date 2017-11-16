/*
 * btreefilescan.C - function members of class BTreeFileScan
 *
 * Spring 14 CS560 Database Systems Implementation
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 
 */

#include "minirel.h"
#include "buf.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btreefilescan.h"

/*
 * Note: BTreeFileScan uses the same errors as BTREE since its code basically 
 * BTREE things (traversing trees).
 */

BTLeafPage *scan_leaf=new BTLeafPage();
char  out_scan[220];
int flag_init=0;
RID first,next;
BTreeFileScan::~BTreeFileScan()
{
  		
 		this->begin=0;
 		this->end=0;
// 		delete scan_leaf;
 		flag_init=0;
  		// put your code here
}


Status BTreeFileScan::get_next(RID & rid, void* keyptr)
{

  	Status  leaf_Read;
  	char *recPtr_comp;
  	int rec_Len;
  	PageId pageNo;
  	Page *leaf_read=new Page();
  	RID curr;
  // cout<<"test enter scan1 "<<endl//;
  	if(this->begin==-1)  return DONE;
  //	if(this->R_Start==this->R_End) return DONE;
 
  	if(!flag_init)
  	{
    //  cout<<"test enter scan "<<endl;
  		 pageNo=this->begin;
  		//  pageNo=35;
  		 leaf_Read=MINIBASE_DB->read_page(pageNo,leaf_read);
  		// scan_leaf=(BTLeafPage *)leaf_read;
  		 memcpy(scan_leaf,leaf_read,sizeof(Page));
  		 scan_leaf->firstRecord(first);
  		 while(first.slotNo<this->R_Start.slotNo) { scan_leaf->nextRecord(first,next); first=next; }

       if(scan_leaf->nextRecord(first,next)!=OK)
       {
            pageNo=scan_leaf->getPrevPage();
           leaf_Read=MINIBASE_DB->read_page(pageNo,leaf_read);
      // scan_leaf=(BTLeafPage *)leaf_read;
           memcpy(scan_leaf,leaf_read,sizeof(Page));
            scan_leaf->firstRecord(first);

       }
    //   first=this->R_Start;
       
  		 scan_leaf->HFPage::returnRecord(first,recPtr_comp,rec_Len);
  		if(this->keytype!=attrString)
        {
            Key_Int *a=(Key_Int *) recPtr_comp;
          //  keyptr=(void *)(&a->intkey);
            memcpy(keyptr,&(a->intkey),sizeof(int));
            rid=a->data.rid;
          //  cout<<"first key "<<a->intkey<<endl;
        }
        else  if(this->keytype==attrString)
        {
            Key_string *a=(Key_string *) recPtr_comp;
          //   strcpy(out_scan,a->charkey.c_str());
            memcpy(keyptr,a->charkey,sizeof(a->charkey));
           // keyptr=(void *)a->charkey.c_str();
            rid=a->data.rid;
        //     cout<<"first key "<<endl;
        }

    //    cout<<" leaf node page number "<<pageNo<<" slot no "<<first.slotNo<<endl;

      //  this->Curr_rid=rid;

        flag_init=1;
  	}
  	else
  	{
       
  	   //  scan_leaf->dumpPage();
  		if(scan_leaf->nextRecord(first,next)==OK)
  		{

  		  if(next.pageNo==this->end&&next.slotNo>this->R_End.slotNo) return DONE;
  	       scan_leaf->HFPage::returnRecord(next,recPtr_comp,rec_Len);

  	     if(this->keytype!=attrString)
      	  {
            Key_Int *a=(Key_Int *) recPtr_comp;
          //   keyptr=(void *)(&(a->intkey));

            memcpy(keyptr,&(a->intkey),sizeof(int));
           //  keyptr=&(a->intkey);
             rid=a->data.rid;

           //  cout<<"key ="<<a->intkey<<" next "<<endl;
      	  }
        else  if(this->keytype==attrString)
      	  {
            Key_string *a=(Key_string *) recPtr_comp;
         //   strcpy(out_scan,a->charkey.c_str());
            memcpy(keyptr,a->charkey,sizeof(a->charkey));
           // keyptr=(void *)a->charkey.c_str();
            rid=a->data.rid;
       	 }
       	 first=next;

//       	 this->Curr_rid=rid;
  	   }
  	   else
  	   {
  	   		 // PageId next_page=scan_leaf->getNextPage();
  	   		//  if(next_page<0)
  	   		 PageId	next_page=scan_leaf->getPrevPage();
  	   		 if(next_page<0) return DONE;
  	   //		 if(next_page<this->end&&this->end!=500) return DONE;
  	   //		  cout<<" pre page  "<<next_page<<endl;
  	   		 leaf_Read=MINIBASE_DB->read_page(next_page,leaf_read);
  	   		//  memcpy(scan_leaf,leaf_read,sizeof(Page));
  			  scan_leaf=(BTLeafPage *)leaf_read;
          if(scan_leaf->empty()) return DONE;
  			  scan_leaf->firstRecord(first);
       
  			 if(next_page==this->end&&first.slotNo>=this->R_End.slotNo) return DONE;
  			 scan_leaf->HFPage::returnRecord(first,recPtr_comp,rec_Len);
  		  	if(this->keytype==attrInteger)
      		  {
                Key_Int *a=(Key_Int *) recPtr_comp;
                memcpy(keyptr,&(a->intkey),sizeof(int));
             //  keyptr=(void *)(&a->intkey);
                rid=a->data.rid;
       		 }
       		 else  if(this->keytype==attrString)
       		 {
            Key_string *a=(Key_string *) recPtr_comp;
           //   strcpy(out_scan,a->charkey.c_str());
            memcpy(keyptr,a->charkey,sizeof(a->charkey));
           // keyptr=(void *)a->charkey.c_str();
            rid=a->data.rid;
       		 }

       	 //  this->Curr_rid=rid;
  	   }

  	}
     
  //   Curr_rid.slotNo=1;
  	delete leaf_read;
  	

  // put your code here
  return OK;
}


Status BTreeFileScan::delete_current()
{
     
    // cout<<"page number "<<first.pageNo<<" slot number "<<first.slotNo<<endl;

     return scan_leaf->HFPage::deleteRecord(first);

  // put your code here
 // return OK;
}


int BTreeFileScan::keysize() 
{
	 if(this->keytype==attrInteger)
	 	return sizeof(int);
	 else
	 	if(this->keytype==attrString)
	 		return 220;
  // put your code here
  return OK;
}
