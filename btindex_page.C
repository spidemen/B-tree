/*
 * btindex_page.C - implementation of class BTIndexPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "btindex_page.h"

// Define your Error Messge here
const char* BTIndexErrorMsgs[] = {
  //Possbile error messages,
  //OK,
  //Record Insertion Failure,
};
RID  first_index;
static error_string_table btree_table(BTINDEXPAGE, BTIndexErrorMsgs);

Status BTIndexPage::insertKey (const void *key,
                               AttrType key_type,
                               PageId pageNo,
                               RID& rid)
{

     int recLen,pentry_len;
     char *recPtr;
    KeyDataEntry target;
    Datatype data;
     Status Index_insert;
    data.pageNo=pageNo;
     make_entry(&target,key_type,key,INDEX,data,&pentry_len);


     if(key_type==attrInteger)
     {
         Key_Int a;
         a.intkey=target.key.intkey;
         a.data.pageNo=pageNo;
        recPtr=(char*)(&a);

       recLen=get_key_data_length(key,key_type,INDEX);
      Index_insert=SortedPage::insertRecord ( key_type,recPtr,recLen,rid);
      
     }
    else if(key_type==attrString)
    {
        Key_string a;
       memcpy(a.charkey,target.key.charkey,sizeof(a.charkey));
      //  a.charkey=target.key.charkey;
        a.data.pageNo=target.data.pageNo;
         recPtr=(char*)(&a);
      //  cout<<"a key size "<< a.charkey.size()<<a.charkey<<endl;
       //   recLen=get_key_data_length(key,key_type,INDEX);
            recLen=sizeof(a);
        Index_insert=SortedPage::insertRecord ( key_type,recPtr,recLen,rid);
  
    }
  //   recPtr=(char*)(&target);
 //    recLen=sizeof(target.key)+sizeof(target.data);
  //   recLen=get_key_data_length(key,key_type,INDEX);
  //   Status Index_insert=SortedPage::insertRecord ( key_type,recPtr,recLen,rid);
     if(Index_insert!=OK) return DONE;
    
  // put your code here
  // put your code here
  return OK;
}

Status BTIndexPage::deleteKey (const void *key, AttrType key_type, RID& curRid)
{

      

  // put your code here
  return OK;
}

Status BTIndexPage::get_page_no(const void *key,
                                AttrType key_type,
                                PageId & pageNo)
{
        RID first,next;
        Status get_rid;
     //   cout<<"test enter: get first rid of HFpage "<<endl; 
        get_rid=SortedPage::firstRecord(first);
        if(get_rid!=OK)   
        {
        //  cout<<"Error: get first rid of HFpage "<<endl; 
          return DONE;
        }
        char *recPtr_comp;
        void *key2;
        int rec_Len,flag_find=0;
        while(SortedPage::nextRecord(first,next)==OK)
        {
           HFPage::returnRecord(first,recPtr_comp,rec_Len);
           if(key_type==attrInteger)
           {
       //     entry_len=4;
            Key_Int *a=(Key_Int *) recPtr_comp;
        //    cout<<a->intkey<<" compare key "<<endl;
            key2=(void *)(&a->intkey);
             if(keyCompare(key, key2, key_type)<=0)
              {
                  pageNo=a->data.pageNo;
                  flag_find=1;
                  break;
              }
          }
          else  if(key_type==attrString)
          {
     //       entry_len=8;
            Key_string *a=(Key_string *) recPtr_comp;
            key2=(void *)(a->charkey);

             if(keyCompare(key, key2, key_type)<=0)
              {
                  pageNo=a->data.pageNo;
                    flag_find=1;
                  break;
              }
          } 
          first=next;
      }


       if(!flag_find)
        {
          HFPage::returnRecord(first,recPtr_comp,rec_Len);
          if(key_type==attrInteger)
           {
            
            Key_Int *a=(Key_Int *) recPtr_comp;
            key2=(void *)(&a->intkey);
             if(keyCompare(key, key2, key_type)<=0)
              {
              //   cout<<a->intkey<<" compare key "<<endl;
                  pageNo=a->data.pageNo;
                   flag_find=1;
              //   cout<<"search in the index page  left "<<pageNo<<endl;
              }
              else
              {
                  pageNo=a->data.pageNo+1;
               //   cout<<"search in the index page  rigth "<<pageNo<<endl;
              }
          }
          else  if(key_type==attrString)
          {
            Key_string *a=(Key_string *) recPtr_comp;
            key2=(void *)a->charkey;

             if(keyCompare(key, key2, key_type)<=0)
              {
                  pageNo=a->data.pageNo;
                    flag_find=1;
                 
              }
          }
      }
       if(!flag_find) return FAIL;
  // put your code here
     return OK;
}

    
Status BTIndexPage::get_first(RID& rid,
                              void *key,
                              PageId & pageNo)
{
   //   RID  first;
      char *recPtr_comp;
      int rec_Len;
       if(SortedPage::firstRecord(first_index)!=OK) cout<<"can not find first record in the first index page "<<endl;
       if(HFPage::returnRecord(first_index,recPtr_comp,rec_Len)!=OK)  cout<<"can not get record from the first index page "<<endl;
       if(this->keytype==attrInteger)
           {
                  Key_Int *a=(Key_Int *) recPtr_comp;
                   pageNo=a->data.pageNo;
                    memcpy(key,&(a->intkey),sizeof(int));
              //    key=(void *)(&a->intkey);
              //  cout<<"Index get first test  page no"<<pageNo<<"  key "<<a->intkey<<endl;
               
          }
          else  if(this->keytype==attrString)
          {
                 Key_string *a=(Key_string *) recPtr_comp;       
                  pageNo=a->data.pageNo;
            //      key=(void *)(a->charkey.c_str());
                memcpy(key,&a->charkey,sizeof(a->charkey));
              //    cout<<"Index get first test string  page no"<<pageNo<<"  key "<<a->charkey.c_str()<<endl;
          }
          rid=first_index;


  // put your code here
  return OK;
}

Status BTIndexPage::get_next(RID& rid, void *key, PageId & pageNo)
{
        RID next;
      char *recPtr_comp;
      int rec_Len;
        if(HFPage::nextRecord(first_index,next)!=OK)
          return DONE;
        else
        {
            first_index=next;
           if(HFPage::returnRecord(first_index,recPtr_comp,rec_Len)!=OK)  cout<<"can not get record from the first index page "<<endl;
            if(this->keytype==attrInteger)
           {
            
                  Key_Int *a=(Key_Int *) recPtr_comp;
                  pageNo=a->data.pageNo;
                    memcpy(key,&(a->intkey),sizeof(int));
              //    key=(void *)(&a->intkey);
            //      cout<<"Index get first test  page no"<<pageNo<<"  key "<<a->intkey<<endl;
               
          }
          else  if(this->keytype==attrString)
          {
                 Key_string *a=(Key_string *) recPtr_comp;       
                  pageNo=a->data.pageNo;
                //  key=(void *)(a->charkey.c_str());
                 memcpy(key,&a->charkey,sizeof(a->charkey));
             //     cout<<"Index get first test  page no"<<pageNo<<"  key "<<a->charkey.c_str()<<endl;
          }
        }

  // put your code here
  return OK;
}
