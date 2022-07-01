#ifndef MINISQL_TRANSACTION_H
#define MINISQL_TRANSACTION_H

#include "page/page.h"
#include <vector>

/**
 * Transaction tracks information related to a transaction.
 *
 * Implemented by student self
*/

enum class TransactionState { GROWING, SHRINKING, COMMITTED, ABORTED };

class TransactionManager;

// class TransactionEffect//effect on pages that the transaction makes
// {
// public:
//     explicit TransactionEffect(page_id_t pid, char* old_page_data)
//         : pid_(pid)
//     {
//         if(old_page_data == nullptr)
//             old_page_data_ = nullptr;
//         else
//         {
//             old_page_data_ = new char[PAGE_SIZE];
//             memcpy(old_page_data_, old_page_data, PAGE_SIZE);
//         }
//     }
//     ~TransactionEffect()
//     {
//         //std::cout<<"~transaction eff"<<std::endl;
//         delete[] old_page_data_;
//         old_page_data_ = nullptr;
//     }
//     page_id_t GetPid(){return pid_;};
//     char* GetData(){return old_page_data_;}
// private:
//     page_id_t pid_;//the id of the page whose content is changed by transaction 
//     char* old_page_data_;//null if create a new page
// };

class Transaction {
public:
    explicit Transaction(txn_id_t txn_id = 0)
        :txn_id_(txn_id), state_(TransactionState::GROWING){}
    ~Transaction()
    {
        //std::cout<<"~transaction"<<std::endl;
        // for(auto teff : effects_)
        //     delete teff;
    }

    txn_id_t GetTid(){return txn_id_;}
    void SetState(TransactionState st){state_ = st;}
    TransactionState GetState(){return state_;}

    // //std::vector<TransactionEffect*>  GetEffects(){return effects_;}
    // void AddEffect(page_id_t pid, char* old_page_data)
    // {
    //     std::cout<<"add effect on page "<<pid<<std::endl;
    //     TransactionEffect* teff = new TransactionEffect(pid, old_page_data);
    //     effects_.push_back(teff);
    // }

    //std::vector<TransactionEffect*> effects_;
private:
    txn_id_t txn_id_;
    TransactionState state_;
};

#endif  // MINISQL_TRANSACTION_H
