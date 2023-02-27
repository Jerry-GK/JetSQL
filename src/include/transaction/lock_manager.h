#ifndef MINISQL_LOCK_MANAGER_H
#define MINISQL_LOCK_MANAGER_H

#include "transaction/transaction.h"
/**
 * LockManager handles transactions asking for locks on pages.
 *
 */
class LockManager 
{
public:
    void SLock(page_id_t pid, Transaction* txn);
    void SUnlock(page_id_t pid, Transaction* txn);
    void Xlock(page_id_t pid, Transaction* txn);
    void XUnlock(page_id_t pid, Transaction* txn);
private:
};

#endif //MINISQL_LOCK_MANAGER_H
