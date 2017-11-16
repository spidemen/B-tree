/*
 * btleaf_page.C - implementation of class BTLeafPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "btleaf_page.h"

const char* BTLeafErrorMsgs[] = {
// OK,
// Insert Record Failed,
};
static error_string_table btree_table(BTLEAFPAGE, BTLeafErrorMsgs);
   
/*
 * Status BTLeafPage::insertRec(const void *key,
 *                             AttrType key_type,
 *                             RID dataRid,
 *                             RID& rid)
 *
 * Inserts a key, rid value into the leaf node. This is
 * accomplished by a call to SortedPage::insertRecord()
 * The function also sets up the recPtr field for the call
 * to SortedPage::insertRecord() 
 * 
 * Parameters:
 *   o key - the key value of the data record.
 *
 *   o key_type - the type of the key.
 * 
 *   o dataRid - the rid of the data record. This is
 *               stored on the leaf page along with the
 *               corresponding key value.
 *
 *   o rid - the rid of the inserted leaf record data entry.
 */


RID  first_leaf;
char  key_scan[220];
Status BTLeafPage::insertRec(const void *key,
                              AttrType key_type,
                              RID dataRid,
                              RID& rid)
{

     int recLen,pentry_len;
     char *recPtr;
      Status Leaf_Insert;
    KeyDataEntry target;
    Datatype data;
     data.pageNo=dataRid.pageNo;
     make_entry(&target,key_type,key,INDEX,data,&pentry_len);
     if(key_type==attrInteger)
     {
         Key_Int a;
         a.intkey=target.key.intkey;
         a.data.rid=dataRid;
         recPtr=(char*)(&a);
         recLen=get_key_data_length(key,key_type,LEAF);
       Leaf_Insert=SortedPage::insertRecord(key_type,recPtr,recLen,rid);
     }
    else if(key_type==attrString)
    {
          Key_string a;
      //  a.charkey=target.key.charkey;
   //     char *c=(char *)key;
   //      string d=c;
   //      a.charkey=d;     
          memcpy(a.charkey,target.key.charkey,sizeof(a.charkey));
         a.data.rid=dataRid;
         recPtr=(char*)(&a);
      //   void *key2=(void *)d.c_str();
      //   cout<<"key "<<d<<endl;
       //  recLen=get_key_data_length(key,key_type,LEAF);
       recLen=sizeof(a);
       Leaf_Insert=SortedPage::insertRecord(key_type,recPtr,recLen,rid);
      // cout<<"key length "<<recLen<<endl;

    }
  //   recPtr=(char*)(&target);
 //    recLen=sizeof(target.key)+sizeof(target.data);
#if 0
     if(key_type==attrString)
     {
        char *recPtr_comp;
     
        SortedPage::returnRecord(rid,recPtr_comp,recLen);
         Key_string *a=(Key_string *) recPtr_comp;
         cout<<"key  ="<<"page no"<<a->data.pageNo<<"  "<<a->charkey<<endl;
      }
#endif

     if(Leaf_Insert!=OK)  return FILEEOF;
      if(HFPage::available_space()<500) return DONE;    // 50% percentage full
    
  // put your code here
    return OK;
}

/*
 *
 * Status BTLeafPage::get_data_rid(const void *key,
 *                                 AttrType key_type,
 *                                 RID & dataRid)
 *
 * This function performs a binary search to look for the
 * rid of the data record. (dataRid contains the RID of
 * the DATA record, NOT the rid of the data entry!)
 */

Status BTLeafPage::get_data_rid(const void *key,
                                AttrType key_type,
                                RID & dataRid)
{

    int low,high,media,rec_Len;
    RID first,next,curRId,nextRid;
    void *key1;
    if(SortedPage::firstRecord(first)!=OK) 
     return DONE;
    low=first.slotNo;
    high=HFPage::slotCnt-1;
    char *recPtr_comp;
    int flag_low=0,flag_high=0;
    curRId=first;
    while(HFPage::nextRecord(curRId,nextRid)!=DONE)
    {
        HFPage::returnRecord(curRId,recPtr_comp,rec_Len);
        if(key_type==attrInteger)
        {
            Key_Int *a=(Key_Int *) recPtr_comp;
            key1=(void *)(&a->intkey);
     //    cout<<"search leaf node  key "<<a->intkey<<"high  "<<high<<" media "<<media<<" low "<<low<<endl;
        }
        else  if(key_type==attrString)
        {
            Key_string *a=(Key_string *) recPtr_comp;
           // key1=(void *)a->charkey.c_str();
             key1=(void *)a->charkey;
        }
          if(keyCompare(key, key1, key_type)==0)
          {
               dataRid=curRId;
                return OK;
          }
          else
             if(keyCompare(key, key1, key_type)<0)
             {
                 dataRid=curRId;
                 return DONE;
             }

         curRId=nextRid;
    }

       HFPage::returnRecord(curRId,recPtr_comp,rec_Len);
        if(key_type==attrInteger)
        {
            Key_Int *a=(Key_Int *) recPtr_comp;
            key1=(void *)(&a->intkey);
          //  cout<<"search leaf node  key "<<a->intkey<<"high  "<<high<<" media "<<media<<" low "<<low<<endl;
        }
        else  if(key_type==attrString)
        {
            Key_string *a=(Key_string *) recPtr_comp;
          //  key1=(void *)a->charkey.c_str();
              key1=(void *)a->charkey;
        }
          if(keyCompare(key, key1, key_type)==0)
          {
              dataRid=curRId;
                return OK;
          }
          else
             if(keyCompare(key, key1, key_type)<0)
             {
                 dataRid=curRId;
                 return DONE;
             }
            else
              return FAIL;
  #if 0
    while(low<=high)
    {
        media=low+(high-low)/2-1;
        curRId.pageNo=HFPage::curPage;
        curRId.slotNo=media;
        if(media>=0)
        {
        HFPage::nextRecord(curRId,nextRid);
        media=nextRid.slotNo;
        } else 
        { media=first.slotNo; nextRid.slotNo=media; }
      //  HFPage::nextRecord(curRId,nextRid);
      //  media=nextRid.slotNo;
        HFPage::returnRecord(nextRid,recPtr_comp,rec_Len);
        if(key_type==attrInteger)
        {
            Key_Int *a=(Key_Int *) recPtr_comp;
            key1=(void *)(&a->intkey);
            cout<<"search leaf node  key "<<a->intkey<<"high  "<<high<<" media "<<media<<" low "<<low<<endl;
        }
        else  if(key_type==attrString)
        {
            Key_string *a=(Key_string *) recPtr_comp;
            key1=(void *)a->charkey.c_str();
        }
        if(keyCompare(key, key1, key_type)<0)
        {
             if(media==high)  flag_high++;
              if(flag_high>3) return FAIL;
            for(int i=media-2;i>=first.slotNo;i--)
            {

                curRId.slotNo=i;
               if(HFPage::nextRecord(curRId,nextRid)==OK);
                {  high=nextRid.slotNo;
                    break;
                }
            }
            if(media-2<0) high=0;
             if(media==high) { dataRid=nextRid;  return DONE;  }
            
        }
        else
            if(keyCompare(key, key1, key_type)>0)
            {
              if(low==media) flag_low++;  

                curRId.slotNo=media;
                HFPage::nextRecord(curRId,nextRid);
                low=nextRid.slotNo;

                 if(low==media) {   dataRid=nextRid; return DONE; }
              //  low=media+1;
            }
            else
            {
                dataRid=nextRid;
                return OK;
            }
    }
#endif  
    return FAIL;
  // put your code here
 // return OK;
}

/* 
 * Status BTLeafPage::get_first (const void *key, RID & dataRid)
 * Status BTLeafPage::get_next (const void *key, RID & dataRid)
 * 
 * These functions provide an
 * iterator interface to the records on a BTLeafPage.
 * get_first returns the first key, RID from the page,
 * while get_next returns the next key on the page.
 * These functions make calls to RecordPage::get_first() and
 * RecordPage::get_next(), and break the flat record into its
 * two components: namely, the key and datarid. 
 */
Status BTLeafPage::get_first (RID& rid,
                              void *key,
                              RID & dataRid)
{ 

     char *recPtr_comp;
      int rec_Len;
       if(SortedPage::firstRecord(first_leaf)!=OK) cout<<"can not find first record in the first index page "<<endl;
       if(HFPage::returnRecord(first_leaf,recPtr_comp,rec_Len)!=OK)  cout<<"can not get record from the first index page "<<endl;
       if(this->keytype==attrInteger)
           {
            
                  Key_Int *a=(Key_Int *) recPtr_comp;
                    memcpy(key,&(a->intkey),sizeof(int));
                 //     cout<<"first key  "<<a->intkey<<endl;
                    dataRid=a->data.rid;
              //    key=(void *)(&a->intkey);
            //      cout<<"Index get first test  page no"<<pageNo<<"  key "<<a->intkey<<endl;
               
          }
          else  if(this->keytype==attrString)
          {
                 Key_string *a=(Key_string *) recPtr_comp;       
              //    pageNo=a->data.pageNo;
                //  strcpy(key_scan,a->charkey.c_str());
              //   cout<<"get first "<<a->charkey<<endl;
                 memcpy(key,&a->charkey,sizeof(a->charkey));
                   dataRid=a->data.rid;
                //  key=(void *)(a->charkey.c_str());
             //     cout<<"Index get first test  page no"<<pageNo<<"  key "<<a->charkey.c_str()<<endl;
          }
          rid=first_leaf;

  // put your code here
  return OK;
}

Status BTLeafPage::get_next (RID& rid,
                             void *key,
                             RID & dataRid)
{

          RID next;
        char *recPtr_comp;
        int rec_Len;
        if(HFPage::nextRecord(first_leaf,next)!=OK)
          return DONE;
        else
        {
            first_leaf=next;
           if(HFPage::returnRecord(first_leaf,recPtr_comp,rec_Len)!=OK)  cout<<"can not get record from the first index page "<<endl;
            if(this->keytype==attrInteger)
           {
            
                  Key_Int *a=(Key_Int *) recPtr_comp;
                //  pageNo=a->data.pageNo;
                    memcpy(key,&(a->intkey),sizeof(int));
                //    cout<<"next key  "<<a->intkey<<endl;
                   dataRid=a->data.rid;
              //    key=(void *)(&a->intkey);
            //      cout<<"Index get first test  page no"<<pageNo<<"  key "<<a->intkey<<endl;
               
          }
          else  if(this->keytype==attrString)
          {
                 Key_string *a=(Key_string *) recPtr_comp;    
              //   strcpy(key_scan,a->charkey.c_str());
             //    cout<<"get next "<<a->charkey<<endl;   
              //    pageNo=a->data.pageNo;
                  
                  memcpy(key,&a->charkey,sizeof(a->charkey));
                    dataRid=a->data.rid;
                //  key=(void *)(a->charkey.c_str());
             //     cout<<"Index get first test  page no"<<pageNo<<"  key "<<a->charkey.c_str()<<endl;
          }
        }

        rid=first_leaf;
  // put your code here
  return OK;
}
