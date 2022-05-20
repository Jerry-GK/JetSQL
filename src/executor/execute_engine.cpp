#include "executor/execute_engine.h"
#include "glog/logging.h"

#define ENABLE_EXECUTE_DEBUG
ExecuteEngine::ExecuteEngine() {
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context);
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context);
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context);
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context);
    case kNodeShowTables:
      return ExecuteShowTables(ast, context);
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context);
    case kNodeDropTable:
      return ExecuteDropTable(ast, context);
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context);
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context);
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context);
    case kNodeSelect:
      return ExecuteSelect(ast, context);
    case kNodeInsert:
      return ExecuteInsert(ast, context);
    case kNodeDelete:
      return ExecuteDelete(ast, context);
    case kNodeUpdate:
      return ExecuteUpdate(ast, context);
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context);
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context);
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context);
    case kNodeExecFile:
      return ExecuteExecfile(ast, context);
    case kNodeQuit:
      return ExecuteQuit(ast, context);
    default:
      break;
  }
  return DB_FAILED;
}

//--------------------------------Database----------------------------------------------------
dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_file_name = ast->child_->val_;
  if(dbs_.find(db_file_name)!=dbs_.end())//database already exists
  {
    cout << "Error: Database \"" << db_file_name << "\" already exists!" << endl;
    return DB_FAILED;
  }
  DBStorageEngine* new_engine = new DBStorageEngine(db_file_name + ".db", false);//false here for temporary
  dbs_.insert(make_pair(db_file_name, new_engine));
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_file_name =  ast->child_->val_;
  if(dbs_.find(db_file_name)==dbs_.end())//database not exists
  {
    cout << "Error: Database \"" << db_file_name << "\" not exists!" << endl;
    return DB_FAILED;
  }

  //dbs_.find(db_file_name)->second->delete_file();
  delete dbs_.find(db_file_name)->second;
  dbs_.erase(db_file_name);
  if(db_file_name==current_db_)
    current_db_="";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if(dbs_.empty())
  {
    cout<<"No database yet!"<<endl;
    return DB_SUCCESS;
  }
  cout << "All databases: ";
  for (unordered_map<std::string, DBStorageEngine *>::iterator it = dbs_.begin(); it != dbs_.end(); it++) {
    cout << it->first << " ";
  }
  cout << "\nCurrent databse: " << current_db_ << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_file_name = ast->child_->val_;
  if(dbs_.find(db_file_name)==dbs_.end())//database not exists
  {
    cout << "Error: Database \"" << db_file_name << "\" not exists!" << endl;
    return DB_FAILED;
  }
  current_db_ = db_file_name;
  return DB_SUCCESS;
}

//--------------------------------Table----------------------------------------------------
dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if(current_db_=="")
  {
    cout << "Error: No database used!" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  if(tables.empty())
  {
    cout << "No table in database \"" << current_db_ << "\" yet!"<<endl;
    return DB_SUCCESS;
  }
  cout<<"All tables: ";
  for(vector<TableInfo *>::iterator it = tables.begin();it!=tables.end();it++)
  {
    cout << (*it)->GetTableName()<<" ";
  }
  cout << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if(current_db_=="")
  {
    cout << "Error: No database used!" << endl;
    return DB_FAILED;
  }
  ast = ast->child_;
  string table_name=ast->val_;
  TableInfo* tinfo_1;
  if(dbs_[current_db_]->catalog_mgr_->GetTable(table_name, tinfo_1)==DB_SUCCESS)
  {
    cout << "Error: Table \""<<table_name<<"\" already exists!" << endl;
    return DB_TABLE_ALREADY_EXIST;
  }

  ast = ast->next_->child_;
  std::vector<Column *> columns;
  SimpleMemHeap heap;
  int index = 0;
  while (ast!=nullptr && ast->type_==kNodeColumnDefinition)  // get schema information
  {
    pSyntaxNode column_def_root = ast;
    string coloumn_name;
    string type_str;
    string is_unique_str;
    string char_length_str;
    if(column_def_root->val_!=nullptr)
      is_unique_str=column_def_root->val_;
    coloumn_name = column_def_root->child_->val_;
    type_str = column_def_root->child_->next_->val_;

    if (column_def_root->child_->next_->child_ != nullptr)
      char_length_str = column_def_root->child_->next_->child_->val_;

    TypeId tid = Type::getTid(type_str);
    ASSERT(tid != kTypeInvalid, "No such data type!");
    bool is_unique = (is_unique_str == "unique");

    //how to deal with not null?
    uint32_t char_length = atoi(char_length_str.c_str());
    if(tid==kTypeChar)
      columns.push_back(ALLOC_COLUMN(heap)(coloumn_name, tid, char_length, index, false, is_unique));
    else
      columns.push_back(ALLOC_COLUMN(heap)(coloumn_name, tid, index, false, is_unique));
    //cout << "name = "<< coloumn_name << " tid = " << tid << " index = " << index << " len = " << char_length
         //<< " is_unique = " << is_unique << endl;
    ast = ast->next_;
    index++;
  }

  auto schema = std::make_shared<Schema>(columns);
  TableInfo *tinfo_2;//not uesed?

  dberr_t ret = dbs_[current_db_]->catalog_mgr_->CreateTable(table_name, schema.get(), nullptr, tinfo_2);//create table4

  if(ast!=nullptr)//create index on primary key if needed
  {
    ASSERT(ast->type_ == kNodeColumnList, "table syntax tree error!");
    string decl_str = ast->val_;
    ASSERT(decl_str== "primary keys", "not primary key declaration!");

    //create index on primary key
    vector<string> pri_index_keys;
    string pri_index_name = "_PRI_"+table_name+"_";
    pSyntaxNode p_index = ast->child_;
    while(p_index!=nullptr)
    {
      pri_index_keys.push_back(p_index->val_);
      cout << pri_index_keys.back() << " ";
      pri_index_name += p_index->val_;
      pri_index_name += "_";
      p_index = p_index->next_;
    }
    
    IndexInfo *iinfo;
    if(dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, pri_index_name, pri_index_keys, nullptr, iinfo)!=DB_SUCCESS)
      return DB_FAILED;
  }

  return ret;
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if(current_db_=="")
  {
    cout << "Error: No database used!" << endl;
    return DB_FAILED;
  }
  string table_name=ast->child_->val_;
  TableInfo* tinfo_1;
  if(dbs_[current_db_]->catalog_mgr_->GetTable(table_name, tinfo_1)==DB_TABLE_NOT_EXIST)
  {
    cout << "Error: Table \""<<table_name<<"\" not exists!" << endl;
    return DB_TABLE_NOT_EXIST;
  }

  return dbs_[current_db_]->catalog_mgr_->DropTable(table_name);
}

//--------------------------------Index---------------------------------------------------
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if(current_db_=="")
  {
    cout << "Error: No database used!" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  if(tables.empty())
  {
    cout << "No table in database \"" << current_db_ << "\" yet!"<<endl;
    return DB_SUCCESS;
  }

  vector<IndexInfo *> indexes;
  cout<<"Indexes on tables: "<<endl;
  for(vector<TableInfo *>::iterator it = tables.begin();it!=tables.end();it++)
  {
    indexes.clear();
    cout << (*it)->GetTableName() << ": ";
    dbs_[current_db_]->catalog_mgr_->GetTableIndexes((*it)->GetTableName(), indexes);
    if(indexes.empty())
      cout << "<No index>";
    else {
      for (vector<IndexInfo *>::iterator itt = indexes.begin(); itt != indexes.end(); itt++)
        cout << (*itt)->GetIndexName() << " ";
    }
    cout << endl;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if(current_db_=="")
  {
    cout << "Error: No database used!" << endl;
    return DB_FAILED;
  }

  string table_name = ast->child_->next_->val_;
  string index_name = ast->child_->val_;

  pSyntaxNode keys_root = ast->child_->next_->next_;
  vector<string> index_keys;
  pSyntaxNode key = keys_root->child_;
  while(key!=nullptr)
  {
    index_keys.push_back(key->val_);
    key = key->next_;
  }

  //not use index type for now

  IndexInfo* iinfo;
  dberr_t res = dbs_[current_db_]->catalog_mgr_->GetIndex(table_name, index_name, iinfo);
  if(res==DB_TABLE_NOT_EXIST)
  {
    cout << "Error: Table \""<<table_name<<"\" not exists!" << endl;
    return DB_TABLE_NOT_EXIST;
  }
  else if (res == DB_SUCCESS) {
    cout<<"Error: Index \""<<index_name<<"\" on \""<<table_name<<"\" already exists!"<<endl;
    return DB_INDEX_ALREADY_EXIST;
  }

  return dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, index_name, index_keys, nullptr, iinfo);
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if(current_db_=="")
  {
    cout << "Error: No database used!" << endl;
    return DB_FAILED;
  }

  //why not declare which table the dropped index is on??????????

  string index_name=ast->child_->val_;
  vector<TableInfo *> tables;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  for(vector<TableInfo *>::iterator it = tables.begin();it!=tables.end();it++)
  {
    if(dbs_[current_db_]->catalog_mgr_->DropIndex((*it)->GetTableName(), index_name)==DB_SUCCESS)
    return DB_SUCCESS;
  }
  cout << "Error: Index \"" << index_name << "\" not found!" << endl;
  return DB_INDEX_NOT_FOUND;
}

//--------------------------------Operation----------------------------------------------------
dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  return DB_FAILED;
}

//--------------------------------Transaction--------------------------------------------------
dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {//Transaction not implemented yet
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {//Transaction not implemented yet
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {//Transaction not implemented yet
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

//--------------------------------Other----------------------------------------------------
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
}
