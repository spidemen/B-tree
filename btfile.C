/*
 * btfile.C - function members of class BTreeFile 
 * 
 * Johannes Gehrke & Gideon Glass  951022  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "minirel.h"
#include "buf.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btreefilescan.h"

// Define your error message here
const char* BtreeErrorMsgs[] = {
  // Possible error messages
  // _OK
  // CANT_FIND_HEADER
  // CANT_PIN_HEADER,
  // CANT_ALLOC_HEADER
  // CANT_ADD_FILE_ENTRY
  // CANT_UNPIN_HEADER
  // CANT_PIN_PAGE
  // CANT_UNPIN_PAGE
  // INVALID_SCAN
  // SORTED_PAGE_DELETE_CURRENT_FAILED
  // CANT_DELETE_FILE_ENTRY
  // CANT_FREE_PAGE,
  // CANT_DELETE_SUBTREE,
  // KEY_TOO_LONG
  // INSERT_FAILED
  // COULD_NOT_CREATE_ROOT
  // DELETE_DATAENTRY_FAILED
  // DATA_ENTRY_NOT_FOUND
  // CANT_GET_PAGE_NO
  // CANT_ALLOCATE_NEW_PAGE
  // CANT_SPLIT_LEAF_PAGE
  // CANT_SPLIT_INDEX_PAGE
};

/*
class HeadPage{

        PageId  root;
        AttrType keytype;
        int keyLength;
        PageId  index[Max_index];
        int index_page_number;
};

*/
#define MAX_Page 500
BTLeafPage *leaf=new BTLeafPage();
BTIndexPage *index1=new BTIndexPage();
BTIndexPage *root=new BTIndexPage();
int flag_tree_insert=0;
static error_string_table btree_table( BTREE, BtreeErrorMsgs);

BTreeFile::BTreeFile (Status& returnStatus, const char *filename)
{
      int head_start;
     if(MINIBASE_DB->get_file_entry(filename,head_start)==OK)
        {
                Page *head_pag;
               this->header=head_start;
                Status head_pin=MINIBASE_BM->pinPage(head_start,head_pag,1);
              if(head_pin!=OK)
                { cout<<"Error: can not pin the head page  "<<head_pag<<endl;
                 returnStatus=FAIL;
                }
               HeadPage *head=new HeadPage();
               head=(HeadPage *)head_pag;
               this->keytype=head->keytype;
          //     cout<<"enter into Btree file "<<endl;
        }
        else
          cout<<"Fata ERROR: can not find the file entry in the DB"<<endl;
        returnStatus=OK;
  // put your code here
}

BTreeFile::BTreeFile (Status& returnStatus, const char *filename, 
                      const AttrType keytype,
                      const int keysize)
{
        int start_pg,head_start;
      if(MINIBASE_DB->get_file_entry(filename,head_start)==OK)
        {
               this->header=head_start;
               this->index_number=0;
               this->keytype=keytype;
        }
      else
      {
        Status Root=MINIBASE_DB->allocate_page(start_pg);
        Status Head=MINIBASE_DB->allocate_page(head_start);
        if(Head==OK)
        Head=MINIBASE_DB->add_file_entry(filename,head_start);   // add entry 
       if(Head!=OK)
        cout<<"Error: Cannot add file entry "<<filename<<endl;
       Page *head_pag;
       Status head_pin=MINIBASE_BM->pinPage(head_start,head_pag,1);
       if(head_pin!=OK)
       { 
        cout<<"Error: can not pin the head page  "<<head_pag<<endl;
          returnStatus=FAIL;
       }
       HeadPage *head=new HeadPage();
       head->root=start_pg;
       head->keytype=keytype;
       head->keyLength=keysize;
       head->index_page_number=0;
       head->filename=filename;
       this->header=head_start;
       this->index_number=0;
       this->keytype=keytype;
       memcpy(head_pag,head,sizeof(HeadPage));
       flag_tree_insert=0;
    //   strcpy((char *)head_pag,(char *)head);

       root->init(start_pg);
    }

    returnStatus=OK;


  // put your code here
}

BTreeFile::~BTreeFile ()
{


  // put your code here
}

Status BTreeFile::destroyFile ()
{
  
  Status destory_page;
  RID rid;
  void *key;
   if(this->keytype==attrInteger)  
      {
        int a;
        key=(void *)&a;
      }
      else
      {
        string a;
        key=(void *)&a;
      }
  PageId pageNo;
  Page *head_pag;
 
  destory_page=root->get_first(rid,key,pageNo);

  destory_page=MINIBASE_DB->deallocate_page(pageNo);
 // cout<<"destory_page id"<<pageNo<<endl;
  while(root->get_next(rid,key,pageNo)==OK)
  {
      destory_page=MINIBASE_DB->deallocate_page(pageNo);
   //    cout<<"destory_page id"<<pageNo<<endl;
  }

   destory_page=MINIBASE_DB->deallocate_page(5);

    int head_start=this->header;
   Status head_pin=MINIBASE_BM->pinPage(head_start,head_pag,1);
    HeadPage *head=new HeadPage();
    head=(HeadPage *)head_pag;
    destory_page=MINIBASE_DB->deallocate_page(head->root);
  //    cout<<"destory_page id"<<head->root<<endl;
   Status Head=MINIBASE_DB->delete_file_entry(head->filename.c_str());

    MINIBASE_BM->freePage(head_start);

   // delete head;
   // delete root,leaf,index1;

    // put your code here
  return OK;
}

Status BTreeFile::insert(const void *key, const RID rid) {
   /*    
     Page *hpg;
     Status headpin=MINIBASE_BM->pinPage(this->header,hpg,0);
     if(this->index_number==0)
     {

     }
    */
      Page *head_pag;
      HeadPage *head=new HeadPage();
      Status head_pin=MINIBASE_BM->pinPage(this->header,head_pag,1);
       if(head_pin!=OK)
       { cout<<"Error: can not pin the head page  "<<this->header<<endl;
       //   returnStatus=FAIL;
       }
      head=(HeadPage *)head_pag;
      AttrType key_type=this->keytype;
      RID leav_rid,index_rid,root_rid;
      Status leav_insert,index_insert,alloc_page,root_insert, page_write;
      int  leaf_alloc,index_alloc,reclen;
      Page *leaf_write=new Page();
      char *recPtr_comp;
      RID Big_rid,dataRid;
       void *key2=new char[220];
      if(!flag_tree_insert)
      {
         alloc_page=MINIBASE_DB->allocate_page(leaf_alloc);
         if(alloc_page!=OK) cout<<"Error: can not allocate a page for  leavf "<<endl;
         leaf->init(leaf_alloc);
         flag_tree_insert=1;
      }
      int pageNo;
     // root->dumpPage();
      root_insert=root->get_page_no(key,this->keytype,pageNo);
      if(root_insert==OK)
      {
      Page *leaf_read=new Page();
       page_write=MINIBASE_DB->read_page(pageNo,leaf_read);
      if(page_write!=OK)  cout<<"Cann not read the leaf page  check key "<<pageNo<<endl;
       BTLeafPage *leaf1=(BTLeafPage *)leaf_read;
       if(page_write==OK&&leaf1->get_data_rid(key,this->keytype,dataRid)==OK)
        return OK;
       delete leaf_read;
     }

     if(root_insert!=OK)
     {
      leav_insert=leaf->insertRec(key,key_type,rid,leav_rid);
      if(leav_insert!=OK&&leav_insert==DONE)
      {
         //  leaf->dumpPage();
           int keylen;

           leaf->Big_key(key_type,key2,keylen);

           char temp[keylen];
           memcpy(temp,key2,keylen);
           const void *key3=temp;
      //   if(key_type==attrString)  cout<<"biggest key  root insert unfound  "<<(char *)key2<<"  leaf page number="<<leav_rid.pageNo<<endl;
        //  else
      //     cout<<"biggest key  root insert unfound  "<<*(int*)key3<<"  leaf page number="<<leav_rid.pageNo<<endl;
          root_insert=root->insertKey(key3,key_type,leav_rid.pageNo,root_rid);
          if(root_insert!=OK)
          {
              cout<<"root full "<<endl;
            //  exit(0);
          }
        //  if(key_type==attrString) exit(0);
          Page *leaf_write1=new Page();
      //   leaf_write=(Page *)leaf;
           alloc_page=MINIBASE_DB->allocate_page(leaf_alloc);
            if(alloc_page!=OK) cout<<"Error: can not allocate a page for  leavf "<<endl;
           leaf->setPrevPage(leaf_alloc);
         // leaf->dumpPage();
      //    leav_insert=leaf->insertRec(key,key_type,rid,leav_rid);
       //   leaf_write=(Page *)leaf;
            memcpy(leaf_write1,leaf,sizeof(Page));
          page_write=MINIBASE_DB->write_page(leav_rid.pageNo,leaf_write1);
         if(page_write!=OK) cout<<"Error: can not write a leaf page to the disk  page number "<<leav_rid.pageNo<<endl;


            leaf->init(leaf_alloc);
            leaf->setNextPage(leav_rid.pageNo);
       //  leav_insert=leaf->insertRec(key,key_type,rid,leav_rid);

          
         memcpy(leaf_write1,leaf,sizeof(Page));
         page_write=MINIBASE_DB->write_page(leaf_alloc,leaf_write1);
         if(page_write!=OK) cout<<"Erro: can not write a leaf page to the disk  page number "<<leav_rid.pageNo<<endl;
         delete  leaf_write1;

      }
      else
      {
        //  leaf_write=(Page *)leaf;
          memcpy(leaf_write,leaf,sizeof(Page));
          page_write=MINIBASE_DB->write_page(leav_rid.pageNo,leaf_write);
         if(page_write!=OK) cout<<"Error: can not write a leaf page to the disk  page number "<<leav_rid.pageNo<<endl;

      }
    }
   else
    {
        //  Page *leaf_write1=new Page();
     //     cout<<"Can not find in the index page  merge sort "<<pageNo<<endl;
        Status leaf_Read=MINIBASE_DB->read_page(pageNo,leaf_write);
        if(leaf_Read!=OK) cout<<"Cann not read the leaf page  "<<pageNo<<endl;
         BTLeafPage *leaf1=(BTLeafPage *)leaf_write;
         leav_insert=leaf1->insertRec(key,key_type,rid,leav_rid);
          //  leaf1->dumpPage();
         if(leav_insert==FILEEOF)
         {
        //  cout<<"page test "<<endl;
           Page *leaf_write2=new Page();
           Page *leaf_temp=new Page();
           int temp_page;
        //   leaf1->dumpPage();
            alloc_page=MINIBASE_DB->allocate_page(leaf_alloc);
           if(alloc_page!=OK) cout<<"Error: can not allocate a page for  leavf "<<endl;
            temp_page=leaf1->getNextPage();
         //   cout<<"current page "<<leaf1->getNextPage()<<" next page "<<leaf1->getNextPage()<<endl;
            if(temp_page!=-1)
            {
             leaf_Read=MINIBASE_DB->read_page(temp_page,leaf_temp);
            if(leaf_Read!=OK) cout<<"Cann not read the leaf page  "<<temp_page<<endl;
            BTLeafPage *leaf_t=(BTLeafPage *)leaf_temp;
            leaf_t->setPrevPage(leaf_alloc);
            leaf_Read=MINIBASE_DB->write_page(temp_page,leaf_temp);
            if(leaf_Read!=OK) cout<<"Cann not write the leaf page  "<<temp_page<<endl;
            }
           leaf1->setNextPage(leaf_alloc);
           leaf_Read=MINIBASE_DB->read_page(leaf_alloc,leaf_write2);
           if(leaf_Read!=OK) cout<<"Cann not read the leaf page  "<<leaf_alloc<<endl;
            BTLeafPage *leaf2=(BTLeafPage *)leaf_write2;
            leaf2->init(leaf_alloc);
            leaf2->setPrevPage(pageNo);
            leaf2->setNextPage(temp_page);
         //   cout<<"test merge sort "<<endl;
            if(key_type==attrInteger||key_type==attrString)
            {
                Key_Int a;
                Key_string b;
               char *stringkey=new char[220];
               RID dataRid,rid1;
               int insert_prob=0;
              void *key_insert;
               if(key_type==attrInteger)
               {
                key_insert=&a.intkey;
               }
               else
               {
                  key_insert=stringkey;
               }
              
               leaf1->keytype=key_type;
               leaf1->get_first(rid1,key_insert,dataRid);
               leav_insert=leaf2->insertRec(key_insert,key_type,dataRid,leav_rid);
               leaf1->HFPage::deleteRecord(rid1);
               while(leav_insert!=DONE)
               {
               
                  leaf1->get_next(rid1,key_insert,dataRid);
              //   leaf1->HFPage::deleteRecord(dataRid);
            //    cout<<"reinsert  key "<<*(int *)key_insert<<endl;
                 leav_insert=leaf2->insertRec(key_insert,key_type,dataRid,leav_rid);
                 leaf1->HFPage::deleteRecord(rid1);

                   if(!insert_prob&&leaf1->insertRec(key,key_type,rid,leav_rid)!=FILEEOF)
                   {
                    insert_prob=1;
                   }

               }

            //    cout<<"test void key "<<*(int *)key<<endl;

            //    leav_insert=leaf2->insertRec(key,key_type,rid,leav_rid);

            //   if(leav_insert==FILEEOF)  cout<<"Error: can not insert "<<endl;

           int keylen;
           leaf2->Big_key(key_type,key2,keylen);
           char temp[keylen];
           memcpy(temp,key2,keylen);
           const void *key3=temp;
       //     if(key_type==attrInteger)
        //   cout<<"biggest key "<<*(char *)key3<<"leaf page number "<<leav_rid.pageNo<<endl;
     //    else
     //      cout<<"biggest string key "<<(char *)key2<<"leaf page number "<<leav_rid.pageNo<<endl;
           root_insert=root->insertKey(key3,key_type,leav_rid.pageNo,root_rid);
           if(root_insert!=OK)
            {
              cout<<"root full  merge "<<endl;
            }
             leaf_Read=MINIBASE_DB->write_page(pageNo,leaf_write);
              if(leaf_Read!=OK) cout<<"Cann not read the leaf page  "<<pageNo<<endl;
              leaf_Read=MINIBASE_DB->write_page(leaf_alloc,leaf_write2);
              if(leaf_Read!=OK) cout<<"Cann not read the leaf page  "<<leaf_alloc<<endl;

           //     leaf1->dumpPage();
           //     leaf2->dumpPage();



              // test printer a page 


#if 0
         char big_key[220];
     if(key_type==attrString)
        {
            RID first,next;
           int recLen;
             Key_string test;
             char *ptr_string;
             cout<<" page number= "<<pageNo<<"next page "<<leaf1->getNextPage()<<"  previous page  "<<leaf1->getPrevPage()<<endl;
          leaf1->firstRecord(first);
         while(leaf1->nextRecord(first, next)==OK)
          {
        //  leaf1->getRecord(first,(char*)(&test),recLen);
            leaf1->returnRecord(first,ptr_string,recLen);
             Key_string *a=(Key_string *) ptr_string;
          //  strcpy(big_key,a->charkey.c_str());
           cout<<"key "<<a->charkey<<" page no"<<test.data.pageNo<<endl;
       //   cout<<"key        "<<big_key<<" page no"<<test.data.pageNo<<endl;
          first=next;
           }
            leaf1->getRecord(first,(char*)(&test),recLen);
           // cout<<"key "<<test.intkey<<" page no"<<test.data.pageNo<<endl;
             cout<<"key "<<test.charkey<<" page no"<<test.data.pageNo<<endl;

             cout<<" page number= "<<leaf_alloc<<"next page "<<leaf2->getNextPage()<<"  previous page  "<<leaf2->getPrevPage()<<endl;
              leaf2->firstRecord(first);
         while(leaf2->nextRecord(first, next)==OK)
          {
          leaf2->getRecord(first,(char*)(&test),recLen);
     //     strcpy(big_key,test.charkey.c_str());
            cout<<"key "<<test.charkey<<" page no"<<test.data.pageNo<<endl;
       //    cout<<"key       "<<big_key<<" page no"<<test.data.pageNo<<endl;
          first=next;
           }
            leaf2->getRecord(first,(char*)(&test),recLen);
         //   cout<<"key "<<test.intkey<<" page no"<<test.data.pageNo<<endl;
             cout<<"key "<<test.charkey<<" page no"<<test.data.pageNo<<endl;


             // if(pageNo==8) exit(0);
            }
#endif
               
            }

            // cout<<" need merger sort , split "<<endl;

           delete leaf_write2,leaf_temp;

         }
         else
         {
         leaf_Read=MINIBASE_DB->write_page(pageNo,leaf_write);
         if(leaf_Read!=OK) cout<<"Cann not read the leaf page  "<<pageNo<<endl;
         }

         delete leaf_write;

    }

    
  // put your code here
  return OK;
}

Status BTreeFile::Delete(const void *key, const RID rid) {

        Status root_search, leaf_Read,leaf_search;
        RID dataRid;
        PageId  pageNo;
        Page *leaf_read=new Page();

        root_search=root->get_page_no(key,this->keytype,pageNo);
        if(root_search!=OK) { cout<<"can not find the key in the index page "<<endl; return FAIL; }
        leaf_Read=MINIBASE_DB->read_page(pageNo,leaf_read);
        if(leaf_Read!=OK) cout<<"Cann not read the leaf page  "<<pageNo<<endl;
         BTLeafPage *leaf1=(BTLeafPage *)leaf_read;



   #if 0
          cout<<"page number ="<<pageNo<<endl;
           RID first,next;
           int recLen;
             Key_Int test;
          leaf1->firstRecord(first);
         while(leaf1->nextRecord(first, next)==OK)
          {
          leaf1->getRecord(first,(char*)(&test),recLen);
          cout<<"key "<<test.intkey<<" page no"<<test.data.pageNo<<endl;
          first=next;
           }
            leaf1->getRecord(first,(char*)(&test),recLen);
          cout<<"key "<<test.intkey<<" page no"<<test.data.pageNo<<endl;
   #endif


         if(leaf1->get_data_rid(key,this->keytype,dataRid)==OK)
         {
             leaf1->HFPage::deleteRecord(dataRid);
             leaf_Read=MINIBASE_DB->write_page(pageNo,leaf_read);
            if(leaf_Read!=OK) cout<<"Cann not read the leaf page  "<<pageNo<<endl;
         }
         else
         {
          // test for print a a page
    #if 1
          cout<<"page number ="<<pageNo<<endl;
           RID first,next;
           int recLen;
             Key_Int test;
          leaf1->firstRecord(first);
         while(leaf1->nextRecord(first, next)==OK)
          {
          leaf1->getRecord(first,(char*)(&test),recLen);
          cout<<"key "<<test.intkey<<" page no"<<test.data.pageNo<<endl;
          first=next;
           }
            leaf1->getRecord(first,(char*)(&test),recLen);
          cout<<"key "<<test.intkey<<" page no"<<test.data.pageNo<<endl;
      #endif
                pageNo=leaf1->getNextPage();
          //   pageNo=leaf1->getPrevPage();
              leaf_Read=MINIBASE_DB->read_page(pageNo,leaf_read);
              if(leaf_Read!=OK) cout<<"Cann not read the leaf page  "<<pageNo<<endl;
               leaf1=(BTLeafPage *)leaf_read;
              if(leaf1->get_data_rid(key,this->keytype,dataRid)==OK)
              {
                leaf1->HFPage::deleteRecord(dataRid);
               leaf_Read=MINIBASE_DB->write_page(pageNo,leaf_read);
              if(leaf_Read!=OK) cout<<"Cann not read the leaf page  "<<pageNo<<endl;
              }
              else
              {
#if 0
             cout<<"page number ="<<pageNo<<endl;
              RID first,next;
           int recLen;
             Key_Int test;
          leaf1->firstRecord(first);
         while(leaf1->nextRecord(first, next)==OK)
          {
          leaf1->getRecord(first,(char*)(&test),recLen);
          cout<<"key "<<test.intkey<<" page no"<<test.data.pageNo<<endl;
          first=next;
           }
            leaf1->getRecord(first,(char*)(&test),recLen);
          cout<<"key "<<test.intkey<<" page no"<<test.data.pageNo<<endl;
  #endif
                     cout<<"can not find the record in the leaf page    page id "<<pageNo<<endl;
                      return FAIL;
              }
         }
         
  // put your code here
  return OK;
}
    
IndexFileScan *BTreeFile::new_scan(const void *lo_key, const void *hi_key) {
  
      BTreeFileScan  *scan=new BTreeFileScan();
      IndexFileScan  *scan1=NULL;
      RID rid,dataRid;
      Status leaf_page,root_search,leaf_Read,root_search1;
      void *key;
      if(this->keytype==attrInteger)  
      {
        int a;
        key=(void *)&a;
      }
      else
      {
        key=new char[220];
      }
       Page *leaf_read=new Page();
      
      
      PageId  pageNo,PageNo_begin;
       root->keytype=this->keytype;
       scan->keytype=this->keytype;
      if(lo_key==NULL&&hi_key==NULL)
      {
         
          leaf_page=root->get_first(rid,key,pageNo);
          scan->begin=pageNo;
          scan->end=MAX_Page;
          scan->R_Start.slotNo=-1;
          scan->R_End.slotNo=MAX_Page;
          scan1=scan;
        
          return scan;
      }
      else
        if(lo_key==NULL&&hi_key!=NULL)
        {

             leaf_page=root->get_first(rid,key,PageNo_begin);
             root_search=root->get_page_no(hi_key,this->keytype,pageNo);

            if(root_search!=OK) { cout<<"can not find the key in the index page "<<endl;  }
             leaf_Read=MINIBASE_DB->read_page(pageNo,leaf_read);
            if(leaf_Read!=OK) cout<<"Cann not read the leaf page  "<<pageNo<<endl;
            BTLeafPage *leaf1=(BTLeafPage *)leaf_read;
            root_search=leaf1->get_data_rid(hi_key,this->keytype,dataRid);
            if(root_search==OK||root_search==DONE)
            {
               scan->begin=PageNo_begin;
               scan->end=pageNo;
                scan->R_Start.slotNo=-1;
               scan->R_End=dataRid;
               if(root_search==DONE) {dataRid.slotNo--;   scan->R_End=dataRid; }
            //   scan->keytype=attrInteger;
             //   cout<<"finish return scan   last "<<pageNo<<endl;
               return scan;
            }
              else
              {
              
                cout<<"Error: can not find  biggest search key "<<endl;
               scan->begin=PageNo_begin;
               scan->end=pageNo;
               cout<<"end page number "<<pageNo<<"  begin page "<<PageNo_begin<<endl;
              
              }

        }
        else if(lo_key!=NULL&&hi_key==NULL)
        {
            root_search=root->get_page_no(lo_key,this->keytype,pageNo);
            if(root_search!=OK) { cout<<"can not find the key in the index page "<<endl;  }
             leaf_Read=MINIBASE_DB->read_page(pageNo,leaf_read);
            if(leaf_Read!=OK) cout<<"Cann not read the leaf page  "<<pageNo<<endl;
            BTLeafPage *leaf1=(BTLeafPage *)leaf_read;
            root_search=leaf1->get_data_rid(lo_key,this->keytype,dataRid);
            if(root_search==OK||root_search==DONE)
            {
               scan->begin=pageNo;
               scan->end=MAX_Page;
               scan->R_Start=dataRid;
               scan->R_End.slotNo=MAX_Page;
               return scan;
            }
              else
              {
              
                cout<<"Error: can not find  biggest search key "<<endl;
               scan->begin=PageNo_begin;
               scan->end=pageNo;
               cout<<"end page number "<<pageNo<<"  begin page "<<PageNo_begin<<endl;
              
              }
        }
        else 
        {

           Status root_search2;
            
            if(keyCompare(lo_key,hi_key,this->keytype)>0)  
            {
               scan->begin=-1;
               return scan;
            }
            PageId  low_key,higkey;
            RID  rid1,rid2;
            root_search=root->get_page_no(lo_key,this->keytype,low_key);
            if(root_search==OK)
            {
             // { cout<<"can not find the key in the index page "<<endl;  }
            leaf_Read=MINIBASE_DB->read_page(low_key,leaf_read);
            if(leaf_Read!=OK) cout<<"Cann not read the leaf page  "<<low_key<<endl;
            BTLeafPage *leaf1=(BTLeafPage *)leaf_read;
            root_search1=leaf1->get_data_rid(lo_key,this->keytype,rid1);
            }
            else root_search=FAIL;
            root_search=root->get_page_no(hi_key,this->keytype,higkey);
            if(root_search==OK) 
            {
              //{ cout<<"can not find the key in the index page "<<endl;  }
             leaf_Read=MINIBASE_DB->read_page(higkey,leaf_read);
            if(leaf_Read!=OK) cout<<"Cann not read the leaf page  "<<higkey<<endl;
            BTLeafPage *leaf2=(BTLeafPage *)leaf_read;
             root_search2=leaf2->get_data_rid(hi_key,this->keytype,rid2);
           }
           else root_search=FAIL;
            if((root_search==OK||root_search==DONE)&&(root_search2==OK||root_search2==DONE))
            {
               scan->begin=low_key;
               scan->end=higkey;
               scan->R_Start=rid1;
               scan->R_End=rid2;
                 if(root_search1==DONE) {rid1.slotNo--;  scan->R_Start=rid1;  }
                if(root_search2==DONE) {rid2.slotNo--;  scan->R_End=rid2;  }
              if(root_search1==DONE&&root_search2==DONE&&rid1==rid2)  scan->begin=-1;
             //  cout<<"enter both find  low page "<<low_key<<" high pag "<<higkey<<" scan begin page "<<scan->begin<<endl;
               return scan;
            }
            else if(root_search!=OK&&root_search!=DONE)
            {
              scan->begin=-1;
               return scan;
            }
            else
            {
              cout<<"Error: can not find  biggest search key "<<endl;
               scan->begin=PageNo_begin;
               scan->end=pageNo;
               cout<<"end page number "<<pageNo<<"  begin page "<<PageNo_begin<<endl;
              
            }


        }

        delete leaf_read;
        
   // put your code here
      
//    return NULL;
}

int keysize(){
  // put your code here
  return 0;
}
