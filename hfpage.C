
#include <iostream>
#include <stdlib.h>
#include <memory.h>

#include "hfpage.h"
#include "buf.h"
#include "db.h"


// **********************************************************
// page class constructor
#define InitPtr 1000         // inital offset 
#define Slot_size sizeof(slot_t)   //sizeof of slot_t
struct Rec
{
    int ival;
    float fval;
    char name[24];
};
short Max_slot_no=0;;
void HFPage::init(PageId pageNo)
{
    
    this->curPage=pageNo;
    this->nextPage=INVALID_PAGE;
    this->prevPage=INVALID_PAGE;
    this->slotCnt=0;
    this->usedPtr=InitPtr;
    this->freeSpace=MAX_SPACE - DPFIXED+sizeof(this->slot[0]);

    memset(this->data,0,1000);
  //  cout<<"test sizeof DFX="<<this->DPFIXED<<endl;
    // fill in the body
      
}

// **********************************************************
// dump page utlity
void HFPage::dumpPage()
{
    int i;
    cout << "dumpPage, this: " << this << endl;
    cout << "curPage= " << curPage << ", nextPage=" << nextPage << endl;
    cout << "usedPtr=" << usedPtr << ",  freeSpace=" << freeSpace
         << ", slotCnt=" << slotCnt << endl;
   
    for (i=0; i < slotCnt; i++) {
        cout << "slot["<< i <<"].offset=" << slot[i].offset
             << ", slot["<< i << "].length=" << slot[i].length << endl;
    }
}

// **********************************************************
PageId HFPage::getPrevPage()
{
      PageId pre_page=this->prevPage;
      return pre_page;
    // fill in the body
   //  return 0;
}

// **********************************************************
void HFPage::setPrevPage(PageId pageNo)
{
      this->prevPage=pageNo;
    // fill in the body
}

// **********************************************************
PageId HFPage::getNextPage()
{
    PageId next_page=this->nextPage;
     return next_page;
    // fill in the body
   //  return 0;
}

// **********************************************************
void HFPage::setNextPage(PageId pageNo)
{
    this->nextPage=pageNo;
  // fill in the body
}

// **********************************************************
// Add a new record to the page. Returns OK if everything went OK
// otherwise, returns DONE if sufficient space does not exist
// RID of the new record is returned via rid parameter.
Status HFPage::insertRecord(char* recPtr, int recLen, RID& rid)
{


    if(this->freeSpace<((sizeof(slot_t)+recLen))||this->freeSpace<=0)
        return DONE;
    short first_Insert_ptr,slot_offset;
    rid.pageNo=this->curPage;
    rid.slotNo=this->slotCnt;
    this->slotCnt++;
    Max_slot_no++;
    //  Max_N=this->slotCnt;
    first_Insert_ptr=this->usedPtr-recLen;      // get the offset of data
 // cout<<first_Insert_ptr<<endl; // test
    memcpy(&(this->data[first_Insert_ptr]),recPtr,recLen);  //put record into memory
    this->usedPtr=first_Insert_ptr;   //set usedPtr
    //   cout<<this->slotCnt<<endl;
    if(this->slotCnt==1)
    {
        this->slot[0].length=recLen;
        this->slot[0].offset=first_Insert_ptr;
    }
    else
    {
        slot_t *slot_record=new slot_t;
        slot_record->length=recLen;
        slot_record->offset=first_Insert_ptr;
        slot_offset=(this->slotCnt-2)*Slot_size;
        memcpy(&(this->data[slot_offset]),slot_record,sizeof(slot_t));  //put slot into memeory
        //  this->freeSpace=(this->freeSpace)-recLen-sizeof(slot_t);
         //  cout<<"insert  slotoffset"<<slot_record->offset<<"  length "<<slot_record->length<<endl;
    }
    
    this->freeSpace=(this->freeSpace)-recLen-sizeof(slot_t);   //reduce freespace
    return OK;

#if 0
      
        if(this->freeSpace<((sizeof(slot_t)+recLen))||this->freeSpace<=0)
          return DONE;
        short first_Insert_ptr,slot_offset;
        rid.pageNo=this->curPage;
        rid.slotNo=this->slotCnt;
        this->slotCnt++;
        Max_slot_no++;
     //  Max_N=this->slotCnt;
        first_Insert_ptr=this->usedPtr-recLen;      // get the offset of data 
        memcpy(&(this->data[first_Insert_ptr]),recPtr,recLen);  //put record into memory
        this->usedPtr=first_Insert_ptr;   //set usedPtr 
     //   cout<<this->slotCnt<<endl;
        if(this->slotCnt==1)
        {
            this->slot[0].length=recLen;
            this->slot[0].offset=first_Insert_ptr;
        }
        else
        {
           slot_t *slot_record=new slot_t;
           slot_record->length=recLen;
           slot_record->offset=first_Insert_ptr;
           slot_offset=(this->slotCnt-2)*Slot_size;
           memcpy(&(this->data[slot_offset]),slot_record,sizeof(slot_t));  //put slot into memeory 
         //  this->freeSpace=(this->freeSpace)-recLen-sizeof(slot_t);
           cout<<"insert  slotoffset"<<slot_record->offset<<"  length "<<slot_record->length<<endl;
        }
       
        this->freeSpace=(this->freeSpace)-recLen-sizeof(slot_t);   //reduce freespace 
        return OK;
#endif 
}

// **********************************************************
// Delete a record from a page. Returns OK if everything went okay.
// Compacts remaining records but leaves a hole in the slot array.
// Use memmove() rather than memcpy() as space may overlap.
Status HFPage::deleteRecord(const RID& rid)
{
     
        if(rid.slotNo>this->slotCnt||rid.slotNo<0)   //slot and rid number should be offical
    {
        //cout<<rid.slotNo<<"   slotCnt ="<<this->slotCnt<<endl;
        return DONE;  }

    int slot_arry[100],k=0;
    int offset1=this->slot[rid.slotNo].offset;
    slot_arry[k++]=rid.slotNo;
    for(int i=0;i<=this->slotCnt-1;i++)
    {
        if(this->slot[i].offset<offset1&&this->slot[i].length!=-1)
        {  slot_arry[k++]=i;
           // cout<<" slot number after compress "<<i<<endl;
            
        }
    }
    
    int sort_slot[k];
    int key,j;
    int temp;

    for(int i=1;i<k;i++)
    {
        key=this->slot[slot_arry[i]].offset;
        temp=slot_arry[i];
        j=i-1;
        while(j>=0&&this->slot[slot_arry[j]].offset<key)
        {
            
            slot_arry[j+1]=slot_arry[j];
           // this->slot[j+1]=this->slot[j];
            j--;
        }
        slot_arry[j+1]=temp;
    //    this->slot[j+1]=temp;
    }
#if 0
    cout<<"offset  ....."<<this->slot[rid.slotNo].offset<<endl;
  for(int i=0;i<k;i++)
  {
      cout<<"offset  less than delete slot "<<this->slot[slot_arry[i]].offset<<endl;
  }
#endif
#if 1
     int  cover_offset=0,current_length;
    cover_offset=this->slot[rid.slotNo].offset;
    for(int i=1;i<k;i++)
    {
     //  cout<<" i  "<<slot_arry[i]<<" cover offset  "<<cover_offset<<" length ="<<this->slot[slot_arry[i]].length<<endl;
        temp= this->slot[slot_arry[i]].offset;
       current_length=this->slot[slot_arry[i]].length;
       this->slot[slot_arry[i]].offset=cover_offset;
      memcpy(&data[this->slot[slot_arry[i]].offset],&data[temp],this->slot[slot_arry[i]].length);
        if(i==k-1)
        {
            break;
        }
        slot_t *rid_slot=&(this->slot[slot_arry[i]]);
        slot_t *rid_next=&(this->slot[slot_arry[i+1]]);
       cover_offset=temp+(current_length-rid_next->length);
     //   memcpy(&data[this->slot[slot_arry[i]].offset],&data[temp],rid_next->length);
    //    this->slot[slot_arry[i+1]].offset=this->slot[slot_arry[i]].offset;
      //  this->slot[slot_arry[i+2]].offset=cover_offset;
       
     //   memcpy(&(this->data[(i)*Slot_size]),rid_next,sizeof(slot_t));
    }
     if(k==1) {
        this->slot[slot_arry[k-1]].offset=this->slot[slot_arry[k-1]].offset+this->slot[rid.slotNo].length;
        this->usedPtr=this->slot[slot_arry[k-1]].offset;
    }
    else
     this->usedPtr=this->slot[slot_arry[k-1]].offset;
      this->freeSpace=this->freeSpace+this->slot[rid.slotNo].length;
     this->slot[rid.slotNo].length=-1;
   //  cout<<"not final "<<"  offset  usedPtr>"<<this->usedPtr<<endl;
 //  for(int i=0;i<k;i++) cout<<"slot i "<<i<<"length ="<<this->slot[i].length<<endl;
     return OK;
#endif    
#if 0
      if(rid.slotNo==0) 
       {
            this->freeSpace=(this->freeSpace)+this->slot[0].length;
            if(this->slotCnt==(rid.slotNo+1))
           this->usedPtr=this->usedPtr+this->slot[0].length;  // modify first usedPtr when only one record exit 
              memset(&data[this->slot[0].offset],this->slot[0].length,0);    // recycle memory 
               int  cover_offset=0;
             for(int i=0;i<this->slotCnt-1;i++)
            {
                  char slot_char[Slot_size];
                  memcpy(slot_char,&data[(i-1)*Slot_size],Slot_size); 
                  slot_t *rid_slot=(slot_t *)(slot_char);
                  memcpy(slot_char,&data[i*Slot_size],Slot_size); 
                   slot_t *rid_next=(slot_t *)(slot_char);
               
                if(i==0)  
                { 
                   char slot_char[Slot_size];
                  memcpy(slot_char,&data[0],Slot_size); 
                  slot_t *rid_slot=(slot_t *)(slot_char);
                 cover_offset=this->slot[i].offset-(rid_slot->length-this->slot[i].length);
                 memcpy(&data[cover_offset],&data[rid_slot->offset],rid_slot->length);

                 rid_slot->offset=cover_offset;
                  memcpy(&(this->data[(i)*Slot_size]),rid_slot,sizeof(slot_t));      
                 this->slot[0].length=-1;
             //     this->slot[0].length=rid_slot->length;
             //     this->slot[0].offset=cover_offset;
                }
                else
                {
                   cover_offset=rid_slot->offset-(rid_next->length-rid_slot->length);
                   memcpy(&data[cover_offset],&data[rid_next->offset],rid_next->length);
                  rid_next->offset=cover_offset;
                  memcpy(&(this->data[(i)*Slot_size]),rid_next,sizeof(slot_t)); 
                }
             
            }
          //   memset(&(this->data[(this->slotCnt-2)*Slot_size]),sizeof(slot_t),0); 
          //   this->slotCnt--;
         //   this->slot[0].length=-1;
            return OK;

       }
       else
      {
         short offset_rid=rid.slotNo;
         short Begin_slot_address,record_offset;
         char slot_char[Slot_size];
         Begin_slot_address=(offset_rid-1)*Slot_size;
         memcpy(slot_char,&data[Begin_slot_address],Slot_size);   // get slot for memory-slot array
         slot_t *rid_slot=(slot_t *)(slot_char);
         memset(&data[rid_slot->offset],rid_slot->length,0);     //recycle memory 
         this->freeSpace=(this->freeSpace)+rid_slot->length+Slot_size;
           if(this->slotCnt==(rid.slotNo+1))
              this->usedPtr=this->usedPtr+this->slot[0].length;   //modify last recrod for this usePtr
           rid_slot->length=-1;
           memcpy(&data[Begin_slot_address],rid_slot,Slot_size);    //update slot which no reord pointed
              int  cover_offset=0;
         for(int i=offset_rid;i<this->slotCnt-1;i++)
            {
                  char slot_char[Slot_size];
                  memcpy(slot_char,&data[(i-1)*Slot_size],Slot_size); 
                  slot_t *rid_slot=(slot_t *)(slot_char);
                  memcpy(slot_char,&data[i*Slot_size],Slot_size); 
                   slot_t *rid_next=(slot_t *)(slot_char);
               
                cover_offset=rid_slot->offset-(rid_next->length-rid_slot->length);
                  memcpy(&data[cover_offset],&data[rid_next->offset],rid_next->length);
                rid_next->offset=cover_offset;
                memcpy(&(this->data[(i)*Slot_size]),rid_next,sizeof(slot_t)); 
            }
       //   memset(&(this->data[(this->slotCnt-2)*Slot_size]),sizeof(slot_t),0); 
      //    this->slotCnt--;
           return OK;
      }


#endif


}

// **********************************************************
// returns RID of first record on page
Status HFPage::firstRecord(RID& firstRid)
{
  //find the first record 
       firstRid.pageNo=this->curPage;
       if(empty()) return DONE;
       if(this->slot[0].length>0)
       {
        firstRid.slotNo=0;
        return OK;
       }
       short Begin_slot_address,record_offset,i;
       char slot_char[Slot_size];
    //  cout<<"i= "<<i<<"slot "<<this->slotCnt<<endl;
      for(i=0;i<this->slotCnt;i++)
      {
          Begin_slot_address=i*Slot_size;      // next slot in the array
          memcpy(slot_char,&data[Begin_slot_address],Slot_size);   //get slot
          slot_t *rid_slot=(slot_t *)(slot_char);

          if(rid_slot->length>0)
            {
              firstRid.slotNo=i+1;
            //  cout<<"test  next rid "<<rid_slot->offset<<endl;
              break;
            }

    }   
  //  cout<<"i= "<<i<<"slot "<<this->slotCnt<<endl;
    if(i>=this->slotCnt)
      return DONE;
     // fill in the body
     return OK;
}

// **********************************************************
// returns RID of next record on the page
// returns DONE if no more records exist on the page; otherwise OK
Status HFPage::nextRecord (RID curRid, RID& nextRid)
{
     if(curRid.slotNo>this->slotCnt||curRid.slotNo<0)
        return FAIL;
     else
       if(curRid.slotNo==this->slotCnt-1)
       {
       // cout<<"Max slot_no="<<Max_slot_no<<"  this->slot"<<this->slotCnt<<endl;
        return DONE;  }
    nextRid.pageNo=this->curPage;
    short Begin_slot_address,record_offset,i;
    char slot_char[Slot_size];
    for(i=curRid.slotNo;i<(this->slotCnt-1);i++)
    {
          Begin_slot_address=i*Slot_size;      // next slot in the array
          memcpy(slot_char,&data[Begin_slot_address],Slot_size);   //get slot
          slot_t *rid_slot=(slot_t *)(slot_char);
          if(rid_slot->length>0&&rid_slot->length<1000)
            {
           
               nextRid.slotNo=i+1;    //find next record rid number 
            //  cout<<"test  next rid "<<rid_slot->offset<<endl;
              break;
            }
       //     else
         //     return  FAIL;
    }
    if(i>=(this->slotCnt-1))
      return DONE;   // no record rid find 
    else
      return OK;
}

// **********************************************************
// returns length and copies out record with RID rid
Status HFPage::getRecord(RID rid, char* recPtr, int& recLen)
{

     short offset_rid=rid.slotNo;
     short Begin_slot_address,record_offset;
     char slot_char[Slot_size];
     Begin_slot_address=(offset_rid-1)*Slot_size;
     memcpy(slot_char,&data[Begin_slot_address],Slot_size);  // get record from memory 
     slot_t *rid_slot=(slot_t *)(slot_char);  // fomat the char array into slot_t
#if 0     
     if(rid.slotNo>=1)
     cout<<"offset="<<rid_slot->offset<<" length "<<rid_slot->length<<endl;
     else 
      cout<<"offset 0="<<this->slot[0].offset<<" length 0 "<<this->slot[0].length<<endl;
#endif 
     if(rid.slotNo>=1)
     {
      // if(rid.slotNo==1) rid_slot->length=Slot_size;
       memcpy(recPtr,&(this->data[rid_slot->offset]),rid_slot->length);  
       recLen=rid_slot->length;
     }
     else
     {
        memcpy(recPtr,&(this->data[this->slot[0].offset]),rid_slot->length); 
        recLen=this->slot[0].length;
     }

        return OK;
}

// **********************************************************
// returns length and pointer to record with RID rid.  The difference
// between this and getRecord is that getRecord copies out the record
// into recPtr, while this function returns a pointer to the record
// in recPtr.
Status HFPage::returnRecord(RID rid, char*& recPtr, int& recLen)
{
      short offset_rid=rid.slotNo;
     short Begin_slot_address,record_offset;
     char slot_char[Slot_size];
     Begin_slot_address=(offset_rid-1)*Slot_size;    //get offset of record 
     memcpy(slot_char,&data[Begin_slot_address],Slot_size);
     slot_t *rid_slot=(slot_t *)(slot_char);
     if(rid.slotNo>=1)
     {
    
      recPtr=&this->data[rid_slot->offset];  //get first address of record 
      recLen=rid_slot->length;
     }
     else
     {
        recLen=this->slot[0].length;
        recPtr=&this->data[slot[0].offset];
     }


    return OK;
}

// **********************************************************
// Returns the amount of available space on the heap file page
int HFPage::available_space(void)
{
     short space=this->freeSpace;
      if(this->slotCnt==0)
        space=space-4;
        return space;
     
    // fill in the body
     // return 0;
}

// **********************************************************
// Returns 1 if the HFPage is empty, and 0 otherwise.
// It scans the slot directory looking for a non-empty slot.
bool HFPage::empty(void)
{
     short size=this->available_space();
     short diff=1000-size;
     if(diff==0)
      return true;
      else
        return false;
    // fill in the body
    
}



