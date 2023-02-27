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

    
private:
    txn_id_t txn_id_;
    TransactionState state_;
};

#endif  // MINISQL_TRANSACTION_H
