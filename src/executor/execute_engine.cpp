#include "executor/execute_engine.h"
#include <iostream>
#include "glog/logging.h"

extern int row_des_count;

//#define ENABLE_EXECUTE_DEBUG
ExecuteEngine::ExecuteEngine(string engine_meta_file_name) {
  // get existed database from meta file
  engine_meta_file_name_ = engine_meta_file_name;
  engine_meta_io_.open(engine_meta_file_name_, std::ios::in);
  if (!engine_meta_io_.is_open()) {
    // create the meta file
    engine_meta_io_.open(engine_meta_file_name_, std::ios::out);
    engine_meta_io_.close();
    heap_ = new UsedHeap;
    return;
  }
  string db_name;
  while (getline(engine_meta_io_, db_name)) {
    if (db_name.empty()) break;
    //try {
      DBStorageEngine *new_engine = new DBStorageEngine(db_name, false);  // load a existed database
      dbs_.insert(make_pair(db_name, new_engine));
    // } catch (int) {
    //   cout << "[Exception]: Can not initialize databases meta!\n"
    //           "(Meta file not consistent with db file. May be caused by for forced quit.)"
    //        << endl;
    //   exit(-1);
    // }
  }
  if(!dbs_.empty()) 
    current_db_ = dbs_.begin()->first;
  else
    current_db_ = "";

  engine_meta_io_.close();
  heap_ = new UsedHeap;
}

using namespace std;
dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  if (ast == nullptr) {
    return DB_FAILED;
  }

  if(current_db_ != "")
  {
    dbs_[current_db_]->bpm_->CheckAllUnpinned();
    //txn = dbs_[current_db_]->txn_mgr_->Begin();
  }

  //if the single command needs to be regarded as a transaction
  bool is_single_transaction = context->txn_ == nullptr && ast->type_!=kNodeCreateDB
    && ast->type_!=kNodeDropDB && ast->type_!=kNodeShowDB && ast->type_!=kNodeUseDB
    && ast->type_!=kNodeTrxBegin && ast->type_!=kNodeTrxCommit && ast->type_!=kNodeTrxRollback
    && ast->type_!=kNodeExecFile && ast->type_!=kNodeQuit;

  if(USING_LOG && is_single_transaction)
      ExecuteTrxBegin(ast, context);

  dberr_t ret = DB_FAILED;
  switch (ast->type_) {
    case kNodeCreateDB:
      ret = ExecuteCreateDatabase(ast, context);
      break;
    case kNodeDropDB:
      ret = ExecuteDropDatabase(ast, context);
      break;
    case kNodeShowDB:
      ret = ExecuteShowDatabases(ast, context);
      break;
    case kNodeUseDB:
      ret = ExecuteUseDatabase(ast, context);
      break;
    case kNodeShowTables:
      ret = ExecuteShowTables(ast, context);
      break;
    case kNodeCreateTable:
      ret = ExecuteCreateTable(ast, context);
      break;
    case kNodeDropTable:
      ret = ExecuteDropTable(ast, context);
      break;
    case kNodeShowIndexes:
      ret = ExecuteShowIndexes(ast, context);
      break;
    case kNodeCreateIndex:
      ret = ExecuteCreateIndex(ast, context);
      break;
    case kNodeDropIndex:
      ret = ExecuteDropIndex(ast, context);
      break;
    case kNodeSelect:
      ret = ExecuteSelect(ast, context);
      break;
    case kNodeInsert:
      ret = ExecuteInsert(ast, context);
      break;
    case kNodeDelete:
      ret = ExecuteDelete(ast, context);
      break;
    case kNodeUpdate:
      ret = ExecuteUpdate(ast, context);
      break;
    case kNodeTrxBegin:
      ret = ExecuteTrxBegin(ast, context);
      break;
    case kNodeTrxCommit:
      ret = ExecuteTrxCommit(ast, context);
      break;
    case kNodeTrxRollback:
      ret = ExecuteTrxRollback(ast, context);
      break;
    case kNodeExecFile:
      ret = ExecuteExecfile(ast, context);
      break;
    case kNodeQuit:
      ret = ExecuteQuit(ast, context);
      break;
    default:
      break;
  }
  
  if(USING_LOG && is_single_transaction)
    ExecuteTrxCommit(ast, context);

  return ret;
}

//--------------------------------Database----------------------------------------------------
dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  // step 1: check if the database already exist
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end())  // database already exists
  {
    context->output_ += "[Error]: Database \"" + db_name + "\" already exists!\n";
    return DB_FAILED;
  }

  // step 2: create a new database engine
  DBStorageEngine *new_engine = new DBStorageEngine(db_name);
  dbs_.insert(make_pair(db_name, new_engine));

  // step 3: append on the meta file
  engine_meta_io_.open(engine_meta_file_name_, std::ios::app);
  if(!engine_meta_io_.is_open())
  {
    context->output_ += "[Exception]: No meta file!\n";
    return DB_FAILED;
  }
  engine_meta_io_ << db_name << endl;  // add a line
  engine_meta_io_.close();

  //step 4: flush to disk
  if(!new_engine->bpm_->FlushAll())
  {
    context->output_ += "[Exception]: Page flushed failed!\n";
    return DB_FAILED;
  }

  //step 5: set cut_db if no database at first
  if(current_db_=="")
  {
    current_db_ = dbs_.begin()->first;
  }
  if(USING_LOG)
    dbs_[current_db_]->txn_mgr_->CheckPoint();
    
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  // step 1: check if the database exists
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end())  // database not exists
  {
    context->output_ += "[Error]: Database \"" + db_name + "\" not exists!\n";
    return DB_FAILED;
  }

  // step 2: update the executor database engine array
  string db_file_name = dbs_.find(db_name)->second->db_file_name_;
  string db_log_file_name;
  if(USING_LOG)
    db_log_file_name = dbs_.find(db_name)->second->log_mgr_->GetLogFileName();
  delete dbs_.find(db_name)->second;
  dbs_.erase(db_name);
  if (db_name == current_db_) current_db_ = "";

  // step 3: delete the database, free its space(not implemented!)
  // dbs_.find(db_file_name)->second->delete_file();
  if (remove(db_file_name.c_str()) != 0) {
    context->output_ += "[Exception]: Database file \"" + db_file_name + " removed failed!\n";
    return DB_FAILED;
  }
  if(USING_LOG)
    if (remove(db_log_file_name.c_str()) != 0) {
      context->output_ += "[Exception]: Database log \"" + db_log_file_name + "\" removed failed!\n";
      return DB_FAILED;
    }

  // step 4: clear the meta file and rewrite all remained db names (for convinience, difficult to delete a certain line)
  engine_meta_io_.open(engine_meta_file_name_, std::ios::out);
  if(!engine_meta_io_.is_open())
  {
    context->output_ += "[Exception]: No meta file!\n";
    return DB_FAILED;
  }
  // rewrite the meta file
  for (unordered_map<std::string, DBStorageEngine *>::iterator it = dbs_.begin(); it != dbs_.end(); it++) {
    engine_meta_io_ << it->first << endl;
  }
  engine_meta_io_.close();

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  // step 1: check existence
  if (dbs_.empty()) {
    context->output_ += "No database yet!\n";
    return DB_SUCCESS;
  }
  // step 2: traverse the databases and output
  context->output_ += "---------------All databases-------------- \n";
  for (unordered_map<std::string, DBStorageEngine *>::iterator it = dbs_.begin(); it != dbs_.end(); it++) {
    context->output_ += it->first + "\n";
  }
  context->output_ += "\n<Current databse>: " + current_db_ + "\n";
  context->output_ += "---------------------------------------\n";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  // step 1: check existence
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end())  // database not exists
  {
    context->output_ += "[Error]: Database \"" + db_name + "\" not exists!\n";
    return DB_FAILED;
  }

  // step 2: alter current_db
  current_db_ = db_name;
  return DB_SUCCESS;
}

//--------------------------------Table----------------------------------------------------
dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  // step 1: traverse the tables
  if (current_db_ == "") {
    context->output_ += "[Error]: No database used!\n";
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  if (tables.empty()) {
    context->output_ += "No table in database \"" + current_db_ + "\" yet!\n";
    return DB_SUCCESS;
  }
  context->output_ += "---------------All tables----------------\n";
  for (vector<TableInfo *>::iterator it = tables.begin(); it != tables.end(); it++) {
    // can be datailed
    if (it == tables.begin()) context->output_ += "+++++++++++++++++++++++++++\n";
    // show table name
    context->output_ += "<Table name>\n";
    context->output_ += (*it)->GetTableName() + "\n";
    // show column information
    context->output_ += "<Columns>\n";
    for (auto col : (*it)->GetSchema()->GetColumns()) {
      context->output_ +=  col->GetName() + "  \t\t" + Type::getTypeName(col->GetType());  // how to align?
      if (col->GetType() == kTypeChar) context->output_ += "(" + to_string(col->GetLength()) + ")";
      context->output_ += "\n";
    }
    context->output_ += "(" + to_string((*it)->GetSchema()->GetColumnCount()) + " columns in total)" + "\n";
    // show row number
    context->output_ += "<Row number>\n";
    context->output_ += to_string((*it)->GerRowNum()) + "\n";  // to be recorded
    // show indexes
    context->output_ += "<Indexes>\n";
    vector<IndexInfo *> indexes;
    dbs_[current_db_]->catalog_mgr_->GetTableIndexes((*it)->GetTableName(), indexes);
    if (indexes.empty())
      context->output_ += "(No index)\n";
    else {
      for (vector<IndexInfo *>::iterator itt = indexes.begin(); itt != indexes.end(); itt++)
        context->output_ += (*itt)->GetIndexName() + "\n";
      context->output_ += "(" + to_string(indexes.size()) + " indexes in total)\n";
    }
    context->output_ += "\n+++++++++++++++++++++++++++\n";
  }
  context->output_ += "\n(" + to_string(tables.size()) + " tables in total)\n";
  context->output_ += "---------------------------------------\n";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  // step 1: check the used database and table existence
  if (current_db_ == "") {
    context->output_ += "[Error]: No database used!\n";
    return DB_FAILED;
  }
  ast = ast->child_;
  string table_name = ast->val_;
  TableInfo *tinfo_temp;
  if (dbs_[current_db_]->catalog_mgr_->GetTable(table_name, tinfo_temp) == DB_SUCCESS) {
    context->output_ += "[Error]: Table \"" + table_name + "\" already exists!\n";
    return DB_TABLE_ALREADY_EXIST;
  }

  // step 2: generate the schema by the syntax tree
  ast = ast->next_->child_;
  std::vector<Column *> columns;
  UsedHeap heap;
  int index = 0;
  while (ast != nullptr && ast->type_ == kNodeColumnDefinition)  // get schema information
  {
    pSyntaxNode column_def_root = ast;
    string coloumn_name;
    string type_str;
    string is_unique_str;
    string char_length_str;
    if (column_def_root->val_ != nullptr) is_unique_str = column_def_root->val_;
    coloumn_name = column_def_root->child_->val_;
    type_str = column_def_root->child_->next_->val_;

    if (column_def_root->child_->next_->child_ != nullptr) {
      char_length_str = column_def_root->child_->next_->child_->val_;
      if ((int)char_length_str.find('.') != -1 || (int)char_length_str.find('-') != -1)  // handle simple input error
      {
        context->output_ += "[Error]: Illegal input for char length!\n";
        return DB_FAILED;
      }
    }

    TypeId tid = Type::getTid(type_str);
    ASSERT(tid != kTypeInvalid, "No such data type!");
    bool is_unique = (is_unique_str == "unique");

    // how to deal with not null?
    int char_length = atoi(char_length_str.c_str());
    if (!(char_length >= 0))  // handle simple input error
    {
      context->output_ += "[Error]: Illegal input for char length!\n";
      return DB_FAILED;
    }

    if (tid == kTypeChar)
      columns.push_back(
          ALLOC_COLUMN(heap)(coloumn_name, tid, (uint32_t)char_length, index, true, is_unique));  // always nullable
    else
      columns.push_back(ALLOC_COLUMN(heap)(coloumn_name, tid, index, true, is_unique));

    ast = ast->next_;
    index++;
  }
  auto schema = std::make_shared<Schema>(columns);
  TableInfo *tinfo;  // not uesed?

  // step 3: create the table using catalog
  dberr_t ret = dbs_[current_db_]->catalog_mgr_->CreateTable(table_name, schema.get(), nullptr, tinfo);

  // step 4: create index on primary key if exists
  if (ast != nullptr) {
    ASSERT(ast->type_ == kNodeColumnList, "table syntax tree error!");
    string decl_str = ast->val_;
    ASSERT(decl_str == "primary keys", "not primary key declaration!");

    // create index on primary key
    vector<string> pri_index_keys;
    string pri_index_name = "_AUTO_PRI_" + table_name + "_";
    pSyntaxNode p_index = ast->child_;
    while (p_index != nullptr) {
      pri_index_keys.push_back(p_index->val_);
      pri_index_name += p_index->val_;
      pri_index_name += "_";
      p_index = p_index->next_;
    }

    IndexInfo *iinfo;
    if (dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, pri_index_name, pri_index_keys, context->txn_, iinfo) !=
        DB_SUCCESS)
      return DB_FAILED;
  }
  else
    context->output_ += "[Warning]: Created table has no primary key!\n";

  // step 5: create index on unqiue key if exists
  for (auto col : tinfo->GetSchema()->GetColumns()) {
    if (col->IsUnique())  // create auto index
    {
      IndexInfo *iinfo;
      string unique_key = col->GetName();
      string unique_index_name = "_AUTO_UNIQUE_" + table_name + "_" + unique_key + "_";
      vector<string> unique_key_vec;
      unique_key_vec.push_back(unique_key);
      if (dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, unique_index_name, unique_key_vec, context->txn_, iinfo) !=
          DB_SUCCESS)
        return DB_FAILED;
    }
  }
  return ret;
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  // step 1: check existence and drop
  if (current_db_ == "") {
    context->output_ += "[Error]: No database used!\n";
    return DB_FAILED;
  }
  string table_name = ast->child_->val_;
  TableInfo *tinfo_1;
  if (dbs_[current_db_]->catalog_mgr_->GetTable(table_name, tinfo_1) == DB_TABLE_NOT_EXIST) {
    context->output_ += "[Error]: Table \"" + table_name + "\" not exists!\n";
    return DB_TABLE_NOT_EXIST;
  }

  return dbs_[current_db_]->catalog_mgr_->DropTable(table_name);
}

//--------------------------------Index---------------------------------------------------
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  // step 1: get database
  if (current_db_ == "") {
    context->output_ += "[Error]: No database used!\n";
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  if (tables.empty()) {
    context->output_ += "No table in database \"" + current_db_ + "\" yet!\n";
    return DB_SUCCESS;
  }
  // step 2: traverse every table and output the index name (including AUTO key index)
  vector<IndexInfo *> indexes;
  context->output_ += "---------------All indexes on tables---------------\n";
  for (vector<TableInfo *>::iterator it = tables.begin(); it != tables.end(); it++) {
    indexes.clear();
    context->output_ += "Table name: " + (*it)->GetTableName() + ": \n";
    dbs_[current_db_]->catalog_mgr_->GetTableIndexes((*it)->GetTableName(), indexes);
    if (indexes.empty())
      context->output_ += "(No index)\n";
    else {
      for (vector<IndexInfo *>::iterator itt = indexes.begin(); itt != indexes.end(); itt++)
        context->output_ += (*itt)->GetIndexName() + "\n";
    }
    context->output_ += "\n";
  }
  context->output_ += "----------------------------------------------------\n";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  // step 1: get the table
  if (current_db_ == "") {
    context->output_ += "[Error]: No database used!\n";
    return DB_FAILED;
  }

  // step 2: examine the index naming
  string table_name = ast->child_->next_->val_;
  string index_name = ast->child_->val_;
  if (index_name.find("_AUTO") == 0) {
    context->output_ += "[Rejection]: Do not name your index in AUTO index naming formula!\n";
    return DB_FAILED;
  }

  // step 3: generate the index key names
  pSyntaxNode keys_root = ast->child_->next_->next_;
  vector<string> index_keys;
  pSyntaxNode key = keys_root->child_;
  while (key != nullptr) {
    index_keys.push_back(key->val_);
    key = key->next_;
  }

  // step 4: check table and index existence
  IndexInfo *iinfo;
  dberr_t res = dbs_[current_db_]->catalog_mgr_->GetIndex(table_name, index_name, iinfo);
  if (res == DB_TABLE_NOT_EXIST) {
    context->output_ += "[Error]: Table \"" + table_name + "\" not exists!\n";
    return DB_TABLE_NOT_EXIST;
  } else if (res == DB_SUCCESS) {
    context->output_ += "[Error]: Index \"" + index_name + "\" on \"" + table_name + "\" already exists!\n";
    return DB_INDEX_ALREADY_EXIST;
  }

  // step 5: check equivalent indexes (give warning)
  vector<IndexInfo *> indexes;
  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, indexes);
  bool have_equivalent = false;
  string eq_index_name;
  for (auto iinfo : indexes) {
    if(index_keys.size() != iinfo->GetIndexKeySchema()->GetColumnCount())
      continue;
    bool is_equivalent = true;
    uint32_t i = 0;
    for (auto col : iinfo->GetIndexKeySchema()->GetColumns()) {
      if (index_keys[i] != col->GetName()) {
        is_equivalent = false;
        break;
      }
      i++;
    }
    if (is_equivalent) {
      have_equivalent = true;
      eq_index_name = iinfo->GetIndexName();
      break;
    }
  }
  if (have_equivalent) {
    // can be resisted if want
    context->output_ +=
        "[Warning]: Index being created is equivalent to the existed index \"" + eq_index_name + "\" !" + "\n";
  }

  // step 6: check the unique constraint
  TableInfo *tinfo;
  dbs_[current_db_]->catalog_mgr_->GetTable(table_name, tinfo);
  if (index_keys.size() > 1)  // not implement multiple uniqueness check, so no multiple non-primary index
  {
    if (!LATER_INDEX_AVAILABLE) {
      context->output_ +=
          "[Rejection]: Can not create index on non-primary multiple-field key because multiple-uniqueness is not "
          "available!\n";
      return DB_FAILED;
    } else {
      // should add duplicate check using file scan
      context->output_ += "[Warning]: Creating index on multiple-field key. Make sure no duplicate keys!\n";
    }
  } else  // check single field uniqueness declaration
  {
    string index_key_name = index_keys[0];
    for (auto col : tinfo->GetSchema()->GetColumns())  // can not get col by name yet
    {
      if (col->GetName() == index_key_name) {
        if (col->IsUnique())  // match
          break;
        else {
          if (!LATER_INDEX_AVAILABLE) {
            context->output_ += "[Rejection]: Can not create index on a field without uniqueness declaration!\n";
            return DB_FAILED;
          } else {
            // should add duplicate check using file scan
            context->output_ +=
                "[Warning]: Creating index on a field without uniqueness declaration. Make sure no duplicate keys!\n";
          }
        }
      }
    }
  }

  // step 7: create the index
  if (dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, index_name, index_keys, context->txn_, iinfo) != DB_SUCCESS)
    return DB_FAILED;

  // step 8: Initialization:
  // after create a nex index on a table, we have to insert initial entries into the index if the table is not empty!
  TableHeap *table_heap = tinfo->GetTableHeap();
  for (auto it = table_heap->Begin(); it != table_heap->End(); it++) {
    Row row = *it;
    // generate the inserted key
    vector<Field> key_fields;
    for (uint32_t i = 0; i < iinfo->GetIndexKeySchema()->GetColumnCount(); i++) {
      key_fields.push_back(*row.GetField(iinfo->GetIndexKeySchema()->GetColumn(i)->GetTableInd()));
    }
    Row key(key_fields, heap_);
    key.SetRowId(row.GetRowId());  // key rowId is the same as the inserted row

    // do insert entry
    if (iinfo->GetIndex()->InsertEntry(key, key.GetRowId(), context->txn_) != DB_SUCCESS) {
      // find duplicate keys while insert entries while initialze the new index
      context->output_ += "[Error]: Can not create index on fields which already have duplicate values!\n";
      // give up to create the index
      if (dbs_[current_db_]->catalog_mgr_->DropIndex(table_name, index_name) != DB_SUCCESS) {
        context->output_ += "[Exception]: Drop index failed when initialzation failed and trying to roll back!\n";
      }
      return DB_FAILED;
    }
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  // step 1: get database
  if (current_db_ == "") {
    context->output_ += "[Error]: No database used!\n";
    return DB_FAILED;
  }

  // why not declare which table the dropped index is on??????????

  // step 2: check if dropping an AUTO key index (not able!)
  string index_name = ast->child_->val_;
  if (index_name.find("_AUTO") == 0) {
    context->output_ += "[Rejection]: Can not drop an AUTO key index!\n";
    return DB_FAILED;
  }

  // step 3: traverse every table and drop index of that name
  // if there are multiple indexes of the same name on different tables, only drop the one on the first tuple!
  vector<TableInfo *> tables;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  for (vector<TableInfo *>::iterator it = tables.begin(); it != tables.end(); it++) {
    if (dbs_[current_db_]->catalog_mgr_->DropIndex((*it)->GetTableName(), index_name) == DB_SUCCESS) {
      // continue;  //if this holds, drop every index of that name
      return DB_SUCCESS;
    }
  }
  context->output_ += "[Error]: Index \"" + index_name + "\" not found!\n";
  return DB_INDEX_NOT_FOUND;
}

//--------------------------------Operation----------------------------------------------------
dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  // step 1: get table
  if (current_db_ == "") {
    context->output_ += "[Error]: No database used!\n";
    return DB_FAILED;
  }

  string table_name = ast->child_->next_->val_;
  TableInfo *tinfo;
  if (dbs_[current_db_]->catalog_mgr_->GetTable(table_name, tinfo) != DB_SUCCESS) {
    context->output_ += "[Error]: Table \"" + table_name + "\" not exists!\n";
    return DB_TABLE_NOT_EXIST;
  }
  else
  {
    vector<IndexInfo *> iinfos;
    dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, iinfos);
    // step 2: do the row selection
    vector<Row> rows;
    if (SelectTuples(ast->child_->next_->next_, context, tinfo, iinfos, &rows) != DB_SUCCESS)  // critical function
    {
      context->output_ += "[Exception]: Tuple selected failed!\n";
      return DB_FAILED;
    }
    // step 3: do the projection
    vector<Row> selected_rows;
    if (ast->child_->type_ == kNodeAllColumns) {
      for (auto &row : rows) selected_rows.emplace_back(row);
    } else  // project each row
    {
      ASSERT(ast->child_->type_ == kNodeColumnList, "No column list for projection");
      Schema *sch = tinfo->GetSchema();
      for (auto &row : rows) {
        vector<Field> selected_row_fields;
        pSyntaxNode p_col = ast->child_->child_;
        while (p_col != nullptr) {
          ASSERT(p_col->type_ == kNodeIdentifier, "No column identifier");
          string col_name = p_col->val_;
          uint32_t col_index;
          if (sch->GetColumnIndex(col_name, col_index) == DB_COLUMN_NAME_NOT_EXIST) {
            context->output_ += "[Error]: Column \"" + col_name + "\" not exists!\n";
            return DB_COLUMN_NAME_NOT_EXIST;
          }
          selected_row_fields.push_back(*row.GetField(col_index));
          p_col = p_col->next_;
        }
        // Row selected_row(selected_row_fields, heap_);
        selected_rows.emplace_back(selected_row_fields, heap_);
      }
    }

    // step 4: do the output
    //get max width for each field
    uint32_t selected_col_num = 0;
    if (ast->child_->type_ == kNodeAllColumns) {
      selected_col_num = tinfo->GetSchema()->GetColumnCount();
    }
    else
    {
      pSyntaxNode p_col = ast->child_->child_;
      while (p_col != nullptr) {
        selected_col_num++;
        p_col = p_col->next_;
      }
    }

    vector<size_t> max_width(selected_col_num, 0);
    //for title
    if(ast->child_->type_ == kNodeAllColumns)
    {
      for (uint32_t i = 0; i < selected_col_num; i++)
        max_width[i] = tinfo->GetSchema()->GetColumn(i)->GetName().size();
    }
    else
    {
      uint32_t i = 0;
      pSyntaxNode p_col = ast->child_->child_;
      while (p_col != nullptr) {
        max_width[i] = string(p_col->val_).size();
        i++;
        p_col = p_col->next_;
      }
    }
    for (vector<Row>::iterator it = selected_rows.begin(); it != selected_rows.end(); it++) {
      Field *fields = it->GetFields();
      for (size_t i = 0; i < it->GetFieldCount(); i++) {
        if (fields[i].IsNull()) 
        {
          max_width[i] = (max_width[i]>strlen("null")?max_width[i] : strlen("null"));
        }
        else
        {
          max_width[i] = (max_width[i]>fields[i].GetDataStr().size()?max_width[i] : fields[i].GetDataStr().size());
        }
      }
    }

    //generate the bar
    string bar;
    for(auto w:max_width)
    {
      bar += '+';
      string temp(w+2, '-');
      bar += temp;
    }
    bar += '+';
    bar += '\n';
  
    // output the table name and the selected column name
    context->output_ += "<Table>: " + table_name + "\n";
    context->output_ += bar;
    context->output_ += "| ";
    if (ast->child_->type_ == kNodeAllColumns) {
      for (uint32_t i = 0; i < tinfo->GetSchema()->GetColumnCount(); i++)
        context->output_ += set_width(tinfo->GetSchema()->GetColumn(i)->GetName(), max_width[i]);
    } else {
      pSyntaxNode p_col = ast->child_->child_;
      int i = 0;
      while (p_col != nullptr) {
        ASSERT(p_col->type_ == kNodeIdentifier, "No column identifier");
        context->output_ += set_width(string(p_col->val_), max_width[i++]);
        p_col = p_col->next_;
      }
    }
    context->output_ += "\n";
    context->output_ += bar;

    // output the rows
    uint32_t col_num = 0;
    for (vector<Row>::iterator it = selected_rows.begin(); it != selected_rows.end(); it++) {
      Field *fields = it->GetFields();
      context->output_ += "| ";
      for (size_t i = 0; i < it->GetFieldCount(); i++) {
        if (fields[i].IsNull()) context->output_ += set_width("null", max_width[i]);
        // if ((*itt)->IsNull())
        else  // do output
        {
          context->output_ += set_width(fields[i].GetDataStr(), max_width[i]);
        }
      }
      context->output_ += "\n";
      col_num++;
    }
    context->output_ += bar;
    context->output_ += "(" + to_string(col_num) + " rows selected)\n";
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  if (current_db_ == "") {
    context->output_ += "[Error]: No database used!\n";
    return DB_FAILED;
  }

  // step 1: get the table
  string table_name;
  table_name = ast->child_->val_;
  TableInfo *tinfo;
  if (dbs_[current_db_]->catalog_mgr_->GetTable(table_name, tinfo) != DB_SUCCESS) {
    context->output_ += "[Error]: Table \"" + table_name + "\" not exists!\n";
    return DB_TABLE_NOT_EXIST;
  }

  // step 2: generate the inserted row
  Schema *sch = tinfo->GetSchema();  // get schema
  pSyntaxNode p_value = ast->child_->next_->child_;
  vector<Field> fields;  // fields in a row to be inserted
  uint32_t col_num = 0;
  while (p_value != nullptr) {
    AddField(sch->GetColumn(col_num)->GetType(), p_value->val_, fields, context);
    col_num++;
    p_value = p_value->next_;
  }
  if (col_num != sch->GetColumnCount()) {
    context->output_ += "[Error]: Inserted field number not matched!\n";
    return DB_FAILED;
  }
  Row row(fields, heap_);  // the row waiting to be inserted

  // step 3: check the index->unique and pri->not null constraint (return DB_FAILED if constraint vialation happens)
  vector<IndexInfo *> iinfos;
  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, iinfos);

  for (vector<IndexInfo *>::iterator it = iinfos.begin(); it != iinfos.end();
       it++)  // traverse every index on the table
  {
    // generate the inserted key
    vector<Field> key_fields;
    for (uint32_t i = 0; i < (*it)->GetIndexKeySchema()->GetColumnCount(); i++) {
      key_fields.push_back(*row.GetField((*it)->GetIndexKeySchema()->GetColumn(i)->GetTableInd()));
      if (key_fields.back().IsNull() &&
          (*it)->GetIndexName().find("_AUTO_PRI") == 0)  // primary key, field can not be null
      {
        context->output_ += "[Rejection]: Can not assign \"null\" to a field of primary key while doing insertion!\n";
        return DB_FAILED;
      }
    }

    Row key(key_fields, heap_);
    key.SetRowId(row.GetRowId());  // key rowId is the same as the inserted row

    // check if violate unique constraint
    vector<RowId> temp;
    if ((*it)->GetIndex()->ScanKey(key, temp, context->txn_) != DB_KEY_NOT_FOUND) {
      context->output_ += "[Rejection]: Inserted row may cause duplicate entry in the table against index \"" +
                          (*it)->GetIndexName() + "\"!\n";
      return DB_FAILED;
    }
  }

  // step 4: do the insertion (insert tuple + insert each related index ) 
  iinfos.clear();
  if (tinfo->GetTableHeap()->InsertTuple(row, context->txn_))  // insert the tuple, rowId has been set
  {
    // update index(do not forget!)
    dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, iinfos);
    for (vector<IndexInfo *>::iterator it = iinfos.begin(); it != iinfos.end();
         it++)  // traverse every index on the table
    {
      // generate the inserted key
      vector<Field> key_fields;
      for (uint32_t i = 0; i < (*it)->GetIndexKeySchema()->GetColumnCount(); i++) {
        key_fields.push_back(*row.GetField((*it)->GetIndexKeySchema()->GetColumn(i)->GetTableInd()));
      }

      Row key(key_fields, heap_);
      key.SetRowId(row.GetRowId());  // key rowId is the same as the inserted row

      // do insert entry into the index
      if ((*it)->GetIndex()->InsertEntry(key, key.GetRowId(), context->txn_) != DB_SUCCESS) {
        context->output_ += "[Exception]: Insert index(" + (*it)->GetIndexName() +
                            ") entry failed while doing insertion (unexpected duplicate)!\n";
        return DB_FAILED;
      }
    }
    dbs_[current_db_]->catalog_mgr_->SetRowNum(tinfo->GetTableId(), tinfo->GerRowNum() + 1);
    return DB_SUCCESS;
  }
  context->output_ += "[Exception]: Insert failed!\n";
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  // step 1: get table
  if (current_db_ == "") {
    context->output_ += "[Error]: No database used!\n";
    return DB_FAILED;
  }
  string table_name = ast->child_->val_;
  TableInfo *tinfo;
  if (dbs_[current_db_]->catalog_mgr_->GetTable(table_name, tinfo) != DB_SUCCESS) {
    context->output_ += "[Error]: Table \"" + table_name + "\" not exists!\n";
    return DB_TABLE_NOT_EXIST;
  }
  vector<IndexInfo *> iinfos;
  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, iinfos);

  // step 2: do the row selection
  vector<Row> rows;
  if (SelectTuples(ast->child_->next_, context, tinfo, iinfos, &rows) != DB_SUCCESS)  // critical function
  {
    context->output_ += "[Exception]: Tuple selected failed!\n";
    return DB_FAILED;
  }

  // step 3: delete the rows
  for (auto &row : rows) {
    if (tinfo->GetTableHeap()->MarkDelete(row.GetRowId(), context->txn_))  // mark delete the tuple, rowId has been set
    {
      // update index(do not forget!)
      for (vector<IndexInfo *>::iterator it = iinfos.begin(); it != iinfos.end();
           it++)  // traverse every index on the table
      {
        // do remove entry
        vector<Field> key_fields;
        for (uint32_t i = 0; i < (*it)->GetIndexKeySchema()->GetColumnCount(); i++) {
          key_fields.push_back(*row.GetField((*it)->GetIndexKeySchema()->GetColumn(i)->GetTableInd()));
        }
        Row key(key_fields, heap_);
        key.SetRowId(row.GetRowId());  // key rowId is the same as the inserted row

        if ((*it)->GetIndex()->RemoveEntry(key, key.GetRowId(), context->txn_) != DB_SUCCESS) {
          context->output_ += "[Exception]: Remove entry of index \"" + (*it)->GetIndexName() + "\" failed while doing deletion!\n";
          return DB_FAILED;
        }
      }
    } else {
      context->output_ += "[Exception]: Mark delete tuple failed!\n";
      return DB_FAILED;
    }
    tinfo->GetTableHeap()->ApplyDelete(row.GetRowId(), context->txn_);
    dbs_[current_db_]->catalog_mgr_->SetRowNum(tinfo->GetTableId(), tinfo->GerRowNum() - 1);
    // if apply delete failed
    // {
    //   context.out_put_ += "Error: Apply delete tuple failed!\n";
    //   return DB_FAILED;
    // }
  }
  context->output_ += "(" + to_string(rows.size()) + " rows deleted)\n";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  // step 1: get the table
  if (current_db_ == "") {
    context->output_ += "[Error]: No database used!\n";
    return DB_FAILED;
  }
  string table_name = ast->child_->val_;
  TableInfo *tinfo;
  if (dbs_[current_db_]->catalog_mgr_->GetTable(table_name, tinfo) != DB_SUCCESS) {
    context->output_ += "[Error]: Table \"" + table_name + "\" not exists!\n";
    return DB_TABLE_NOT_EXIST;
  }
  Schema *sch = tinfo->GetSchema();
  vector<IndexInfo *> iinfos;
  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, iinfos);

  // step 2: do the row selection
  vector<Row> rows;
  if (SelectTuples(ast->child_->next_->next_, context, tinfo, iinfos, &rows) != DB_SUCCESS)  // critical function
  {
    context->output_ += "[Exception]: Tuple selected failed!\n ";
    return DB_FAILED;
  }

  // step 3: save the column name and update value
  unordered_map<string, pSyntaxNode> update_cols;
  pSyntaxNode p_cols = ast->child_->next_;
  ASSERT(p_cols->type_ == kNodeUpdateValues, "Wrong node for update!");
  p_cols = p_cols->child_;
  ASSERT(p_cols->type_ == kNodeUpdateValue, "Wrong node for update!");
  while (p_cols != nullptr) {
    update_cols.insert(make_pair(p_cols->child_->val_, p_cols->child_->next_));
    uint32_t temp_ind;
    if (sch->GetColumnIndex(p_cols->child_->val_, temp_ind) == DB_COLUMN_NAME_NOT_EXIST) {
      context->output_ += "[Error]: Column \"" + string(p_cols->child_->val_) + "\" not exists!\n";
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    p_cols = p_cols->next_;
  }

  // step 4: generate the updated rows
  vector<Row> new_rows;
  for (auto row : rows) {
    Field *old_fields_p = row.GetFields();
    vector<Field> new_fields;
    int col_index = 0;
    for (size_t i = 0; i < row.GetFieldCount(); i++) {
      unordered_map<string, pSyntaxNode>::iterator it = update_cols.find(sch->GetColumn(col_index)->GetName());
      if (it != update_cols.end())  // update the field
      {
        // update field
        AddField(sch->GetColumn(col_index)->GetType(), it->second->val_, new_fields, context);
      } else  // copy the original field
      {
        new_fields.emplace_back(old_fields_p[i]);
      }
      col_index++;
    }

    // for (auto field_p : old_fields_p) {
    //   unordered_map<string, pSyntaxNode>::iterator it = update_cols.find(sch->GetColumn(col_index)->GetName());
    //   if (it != update_cols.end())  // update the field
    //   {
    //     // update field
    //     AddField(sch->GetColumn(col_index)->GetType(), it->second->val_, new_fields);
    //   } else  // copy the original field
    //   {
    //     new_fields.push_back(*field_p);
    //   }
    //   col_index++;
    // }
    Row new_row(new_fields, heap_);
    new_row.SetRowId(row.GetRowId());
    new_rows.emplace_back(new_row);
  }

  // step 5: check(every updated new row) the index->unique and pri->not null constraint (return DB_FAILED if constraint
  // vialation happens)
  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, iinfos);
  for (auto new_row : new_rows) {
    for (vector<IndexInfo *>::iterator it = iinfos.begin(); it != iinfos.end();
         it++)  // traverse every index on the table
    {
      // generate the inserted key and check the intersaction at the same time
      vector<Field> key_fields;
      for (uint32_t i = 0; i < (*it)->GetIndexKeySchema()->GetColumnCount(); i++) {
        key_fields.push_back(*new_row.GetField((*it)->GetIndexKeySchema()->GetColumn(i)->GetTableInd()));
        if (key_fields.back().IsNull() &&
            (*it)->GetIndexName().find("_AUTO_PRI") == 0)  // primary key, field can not be null
        {
          context->output_ += "[Rejection]: Can not assign \"null\" to a field of primary key while doing update!\n";
          return DB_FAILED;
        }
      }

      Row key(key_fields, heap_);
      key.SetRowId(new_row.GetRowId());  // key rowId is the same as the inserted row

      // check if violate unique constraint
      vector<RowId> scan_res;
      if ((*it)->GetIndex()->ScanKey(key, scan_res, context->txn_) == DB_SUCCESS) {
        ASSERT(!scan_res.empty(), "Scan key succeed but result empty");
        if (scan_res[0] == key.GetRowId())  // It doesn't matter if violates itself (do not forget this point!)
          continue;
        context->output_ += "[Rejection]: Updated row may cause duplicate entry in the table against index \"" +
                            (*it)->GetIndexName() + "\"!\n";
        return DB_FAILED;
      }
    }
  }

  // step 6: update (update index entry as well)
  ASSERT(new_rows.size() == rows.size(), "Rows number not matched!");
  for (uint32_t i = 0; i < new_rows.size(); i++)  // update every selected row
  {
    Row new_row = new_rows[i];
    Row old_row = rows[i];
    // update the row with new row
    if (!tinfo->GetTableHeap()->UpdateTuple(new_row, old_row.GetRowId(), context->txn_)) {
      context->output_ += "[Exception]: Can not update tuple!\n";
      return DB_FAILED;
    }

    // update index entry
    for (vector<IndexInfo *>::iterator it = iinfos.begin(); it != iinfos.end();
         it++)  // traverse every index on the table
    {
      // remove the old entry
      vector<Field> old_key_fields;
      for (uint32_t i = 0; i < (*it)->GetIndexKeySchema()->GetColumnCount(); i++) {
        old_key_fields.push_back(*old_row.GetField((*it)->GetIndexKeySchema()->GetColumn(i)->GetTableInd()));
      }
      Row old_key(old_key_fields, heap_);
      old_key.SetRowId(old_row.GetRowId());
      if (((*it)->GetIndex()->RemoveEntry(old_key, old_key.GetRowId(), context->txn_)) != DB_SUCCESS) {
        context->output_ += "[Exception]: Remove index failed while doing update (may exist duplicate keys)!\n";
        return DB_FAILED;
      }
      vector<Field> new_key_fields;
      for (uint32_t i = 0; i < (*it)->GetIndexKeySchema()->GetColumnCount(); i++) {
        new_key_fields.push_back(*new_row.GetField((*it)->GetIndexKeySchema()->GetColumn(i)->GetTableInd()));
      }
      Row new_key(new_key_fields, heap_);
      new_key.SetRowId(new_row.GetRowId());

      // do insert new entry
      if (((*it)->GetIndex()->InsertEntry(new_key, new_key.GetRowId(), context->txn_)) !=
          DB_SUCCESS)  // why failed(duplicate)?
      {
        context->output_ += "[Exception]: Insert index(" + (*it)->GetIndexName() +
                            ") entry failed while doing update (unexpected duplicate)!\n";
        return DB_FAILED;
      }
    }
  }
  context->output_ += "(" + to_string(new_rows.size()) + " rows updated)\n";
  return DB_SUCCESS;
}

//--------------------------------Transaction(not implemented yet)--------------------------------------------------
dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {  // Transaction not implemented yet
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  if (current_db_ == "") 
  {
    context->output_ += "[Error]: No database used!\n";
    return DB_FAILED;
  }
  if(!USING_LOG)
  {
    context->output_ += "[Error]: Current DBMS mode does NOT support transaction!\n";
    return DB_FAILED;
  }
  if(context->txn_!=nullptr)
  {
    context->output_ += "[Error]: Looped transaction is not available!\n";
    return DB_FAILED;
  }
  context->txn_ = dbs_[current_db_]->txn_mgr_->Begin();
  if(context->txn_ == nullptr)
    cout<<"??"<<endl;
  dbs_[current_db_]->bpm_->SetTxn(context->txn_);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {  // Transaction not implemented yet
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  if (current_db_ == "") {
    context->output_ += "[Error]: No database used!\n";
    return DB_FAILED;
  }
  if(!USING_LOG)
  {
    context->output_ += "[Error]: Current DBMS mode does NOT support transaction!\n";
    return DB_FAILED;
  }
  if(context->txn_==nullptr)
  {
    context->output_ += "[Error]: No running transaction to commit!\n";
    return DB_FAILED;
  }
  dbs_[current_db_]->txn_mgr_->Commit(context->txn_);
  context->txn_ = nullptr;
  dbs_[current_db_]->bpm_->SetTxn(context->txn_);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {  // Transaction not implemented yet
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  if (current_db_ == "") {
    context->output_ += "[Error]: No database used!\n";
    return DB_FAILED;
  }
  if(!USING_LOG)
  {
    context->output_ += "[Error]: Current DBMS mode does NOT support transaction!\n";
    return DB_FAILED;
  }
  if(context->txn_==nullptr)
  {
    context->output_ += "[Error]: No running transaction to rollback!\n";
    return DB_FAILED;
  }
  dbs_[current_db_]->txn_mgr_->Abort(context->txn_);
  dbs_[current_db_]->catalog_mgr_->LoadFromBuffer();//reload after rollback
  context->txn_ = nullptr;
  dbs_[current_db_]->bpm_->SetTxn(context->txn_);
  return DB_SUCCESS;
}

//--------------------------------Other----------------------------------------------------
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif

  // step 1: open the file
  string sql_file_name = ast->child_->val_;
  fstream sql_file_io;
  sql_file_io.open("../doc/sql/" + sql_file_name, ios::in);
  if (!sql_file_io.is_open()) {
    context->output_ += "[Exception]: Can not open file \"" + sql_file_name + "\" in /doc/sql !\n";
    return DB_FAILED;
  }

  // step 2: read each command, parse and execute
  const int buf_size = 1024;
  char cmd[buf_size];
  bool is_file_end = false;
  uint32_t suc_cmd_num = 0;  // number of commands executed successfully

  cout<< "\n---------------------Start Executing File---------------------\n";
  while (true)               // for each command in file
  {
    memset(cmd, 0, buf_size);
    char ch;
    int i = 0;

    while (1)  // read a command
    {
      if (!sql_file_io.get(ch)) {
        is_file_end = true;
        break;
      }
      if(ch == '\r')ch = ' ';
      cmd[i++] = ch;
      if (ch == ';') break;
    }
    sql_file_io.get();  // remove the enter

    if (is_file_end) break;

    string cmd_str(cmd);
    cout << "\n[Executing]: " << cmd_str << endl;

    //  create buffer for sql input
    YY_BUFFER_STATE bp = yy_scan_string(cmd);
    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      exit(1);
    }
    yy_switch_to_buffer(bp);

    // init parser module
    MinisqlParserInit();

    // parse
    yyparse();

    // parse result handle

    if (MinisqlParserGetError())  // the second condition is to avoid strange parser error
    {
      // error
      printf("[Parse Error in File]: %s\n", MinisqlParserGetErrorMessage());
    } else {
#ifdef ENABLE_PARSER_DEBUG
      printf("[INFO] Sql syntax parse ok!\n");
      SyntaxTreePrinter printer(MinisqlGetParserRootNode());
      printer.PrintTree(syntax_tree_file_mgr[0]);
#endif
    }

    ExecuteContext sub_context;
    sub_context.input_ = cmd_str;
    sub_context.txn_ = context->txn_;
    clock_t stm_start = clock();
    if (Execute(MinisqlGetParserRootNode(), &sub_context) != DB_SUCCESS)  // execute the command. eixt if failed
    {
      cout << sub_context.output_ << endl;
      printf("[Failure in File]: SQL statement executed failed!\n");
      MinisqlParserFinish();
      yy_delete_buffer(bp);
      yylex_destroy();
      sql_file_io.close();
      return DB_FAILED;
    } else {
      cout << sub_context.output_ << endl;
      // count time for each statement in file
      clock_t stm_end = clock();
      double run_time = (double)((stm_end - stm_start)) / CLOCKS_PER_SEC;
      printf("[Success in File]: (run time: %.3f sec)\n", run_time);
      suc_cmd_num++;
    }

    // clean memory after parse
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();

    // quit condition
    if (context->flag_quit_) {
      break;
    }
    context->txn_ = sub_context.txn_;
  }

  cout<< "\n---------------------End Executing File---------------------\n";
  context->output_ += "\n(" + to_string(suc_cmd_num) + " statements in file executed successfully)\n";
  sql_file_io.close();
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
}

// my added member function (critical part)
// cond_root_ast: the root node for the Condition Node in syntax tree
// tinfo: the current selected table info
// iinfos: the indexes info of current table
// rows: receive the result
dberr_t ExecuteEngine::SelectTuples(const pSyntaxNode cond_root_ast, ExecuteContext *context, TableInfo *tinfo,
                                    vector<IndexInfo *> iinfos,
                                    vector<Row> *rows)  // select the rows according to the condition node
{
  // step 1: exclude exceptions and get the table heap
  ASSERT(tinfo != nullptr, "Null for select");
  ASSERT(cond_root_ast == nullptr || cond_root_ast->type_ == kNodeConditions, "No condition nodes!");

  TableHeap *table_heap = tinfo->GetTableHeap();

  // step 2: do selection (no condition, single condition, multiple condition)
  if (cond_root_ast == nullptr)  // no condition(return all tuples)
  {
    for (auto it = table_heap->Begin(); it != table_heap->End(); it++)  // traverse tuples
    {
      rows->emplace_back(*it);
    }
  } else if (cond_root_ast->child_->type_ == kNodeCompareOperator)  // single condition
  {
    string col_name = cond_root_ast->child_->child_->val_;
    uint32_t col_ind = -1;
    if (tinfo->GetSchema()->GetColumnIndex(col_name, col_ind) == DB_COLUMN_NAME_NOT_EXIST) {
      context->output_ += "[Error]: Column \"" + col_name + "\" not exists!\n";
      return DB_COLUMN_NAME_NOT_EXIST;
    }

    // check if there is an index to use
    bool have_index = false;
    for (auto iinfo : iinfos) {
      // only use single key index for query optimization now
      if (iinfo->GetIndexKeySchema()->GetColumnCount() == 1 &&
          iinfo->GetIndexKeySchema()->GetColumn(0)->GetName() == col_name) {
        // found an possible index,
        vector<Field> fields;
        AddField(iinfo->GetIndexKeySchema()->GetColumn(0)->GetType(), cond_root_ast->child_->child_->next_->val_,
                 fields, context);
        // no consider for null insertion for index column now!

        Row key(fields, heap_);
        vector<RowId> select_rid;
        BPlusTreeIndex *ind = reinterpret_cast<BPlusTreeIndex *>(iinfo->GetIndex());

        have_index = true;
        context->output_ += "[Note]: Using index \"" + iinfo->GetIndexName() + "\" to select tuples!\n";
        auto correct_target = ind->GetBeginIterator(key);
        auto ls_target = ind->FindLastSmallerOrEqual(key);  // last smaller or equal

        string comp_str(cond_root_ast->child_->val_);
        if (comp_str == "=") {
          if (correct_target == ind->GetEndIterator()) break;  // no equal index for the entry
          if(!correct_target.IsNull())
          {
            Row row((*correct_target).value, heap_);
            table_heap->GetTuple(&row, context->txn_);
            rows->emplace_back(row);
          }
        } else if (comp_str == "<>") {
          for (auto it = ind->GetBeginIterator(); it != ind->GetEndIterator(); ++it)  // return all but not target
          {
            Row row((*it).value, heap_);
            table_heap->GetTuple(&row, context->txn_);
            if (it != correct_target && !it.IsNull()) //can be better
              rows->emplace_back(row);
          }
        } else if (comp_str == ">") {
          auto it = ls_target;
          if(it == ind->GetEndIterator())
            it = ind->GetBeginIterator();
          for (; it != ind->GetEndIterator(); ++it)  // return all larger than target
          {
            if (it == ls_target || it.IsNull()) continue;
            Row row((*it).value, heap_);
            table_heap->GetTuple(&row, context->txn_);
            rows->emplace_back(row);
          }
        } else if (comp_str == ">=") {
          auto it = ls_target;
          if(it == ind->GetEndIterator())
            it = ind->GetBeginIterator();
          for (; it != ind->GetEndIterator(); ++it)  // return all >= target
          {
            if (it != ls_target || correct_target != ind->GetEndIterator() || it.IsNull())  // skip the first iterator if not equal
            {
              Row row((*it).value, heap_);
              table_heap->GetTuple(&row, context->txn_);
              rows->emplace_back(row);
            }
          }
        } else if (comp_str == "<") {
          if(ls_target == ind->GetEndIterator())
            break;
          for (auto it = ind->GetBeginIterator();; ++it)  // return all less than target
          {
            if (it == ind->GetEndIterator()) break;
            if (it == ls_target) {
              if (correct_target != ind->GetEndIterator())  // equal (indicated by valid correct_target), break directly
              {
                break;
              } else  // not equal, include and break
              {
                if(!it.IsNull())
                {
                  Row row((*it).value, heap_);
                  table_heap->GetTuple(&row, context->txn_);
                  rows->emplace_back(row);
                }
                break;
              }
            }
            if(!it.IsNull())
            {
              Row row((*it).value, heap_);
              table_heap->GetTuple(&row, context->txn_);
              rows->emplace_back(row);
            }
          }
        } else if (comp_str == "<=") {
          if(ls_target == ind->GetEndIterator())
            break;
          for (auto it = ind->GetBeginIterator();; ++it)  // return all <= target
          {
            if (it == ind->GetEndIterator()) break;
            if(!it.IsNull())
            {
              Row row((*it).value, heap_);
              table_heap->GetTuple(&row, context->txn_);
              rows->emplace_back(row);
            }
            if (it == ls_target) break;
          }
        } else if (comp_str == "is") {
          if (cond_root_ast->child_->child_->next_->type_ != kNodeNull) {
            context->output_ += "[Exception]: Comparator \"is\" can only fit identifier \"null\" !\n";
            return DB_FAILED;
          }
          if (correct_target != ind->GetEndIterator()) {
            Row row((*correct_target).value, heap_);
            table_heap->GetTuple(&row, context->txn_);
            rows->emplace_back(row);
          }
        } else if (comp_str == "not") {
          if (cond_root_ast->child_->child_->next_->type_ != kNodeNull) {
            context->output_ += "[Exception]: Comparator \"not\" can only fit identifier \"null\" !\n";
            return DB_FAILED;
          }
          for (auto it = ind->GetBeginIterator(); it != ind->GetEndIterator(); ++it)  // return all but not target
          {
            Row row((*it).value, heap_);
            table_heap->GetTuple(&row, context->txn_);
            if (it != correct_target) rows->emplace_back(row);
          }
        } else
          ASSERT(false, "Invalid comparator!");
        break;  // won't come here
      }
    }
    // no available index on single condition column, traverse and examine
    if (!have_index) {
      for (auto it = table_heap->Begin(); it != table_heap->End(); it++)  // traverse tuples
      {
        // check the comparasion
        // Row row = *it;
        if (RowSatisfyCondition(*it, cond_root_ast, tinfo, context)) 
          rows->push_back(*it);
      }
    }
  } else if (cond_root_ast->child_->type_ == kNodeConnector)  // multiple condition
  {
    // file scan now, without possible optimization
    context->output_ += "[Note]: Multiple conditions!\n";
    for (auto it = table_heap->Begin(); it != table_heap->End(); it++)  // traverse tuples
    {
      // check the comparasion
      // Row row = *it;
      if (RowSatisfyCondition(*it, cond_root_ast, tinfo, context)) 
        rows->push_back(*it);
    }
  } else
    ASSERT(false, "Unknown select condition!");
  return DB_SUCCESS;
}

// single comparison function, return true if comparision pass
// f: the field
// p_comp:the comparator (=, !=, >, <, <=, >=, is, not)
bool ExecuteEngine::CompareSuccess(Field *f, pSyntaxNode p_comp, pSyntaxNode p_val, ExecuteContext *context) {
  ASSERT(f != nullptr && p_comp != nullptr && p_val != nullptr, "Compaision failed!");
  string comp_str(p_comp->val_);

  // first check the null condition
  if (comp_str == "is") {
    if (p_val->type_ != kNodeNull) {
      context->output_ += "[Exception]: Comparator \"is\" can only fit identifier \"null\" !\n";
      return false;
    }
    return f->IsNull();
  } else if (comp_str == "not") {
    if (p_val->type_ != kNodeNull) {
      context->output_ += "[Exception]: Comparator \"not\" can only fit identifier \"null\" !\n";
      return false;
    }
    return !f->IsNull();
  }

  if(p_val->type_ == kNodeNull)// like <= null is invalid
  {
    return false;
  }

  if(f->IsNull())//null won't be selected not using "is" or "not"
  {
    return false;
  }

  // generate the compared field
  vector<Field> right_f_;
  AddField(f->GetTypeId(), p_val->val_, right_f_, context);
  ASSERT(!right_f_.empty(), "No right field");

  Field right_f(right_f_[0], heap_);  // because

  if (comp_str == "=") {
    return f->CompareEquals(right_f);
  } else if (comp_str == "<>") {
    return f->CompareNotEquals(right_f);
  } else if (comp_str == ">") {
    return f->CompareGreaterThan(right_f);
  } else if (comp_str == ">=") {
    return f->CompareGreaterThanEquals(right_f);
  } else if (comp_str == "<") {
    return f->CompareLessThan(right_f);
  } else if (comp_str == "<=") {
    return f->CompareLessThanEquals(right_f);
  } else
    ASSERT(false, "Invalid comparator!");

  return false;  // should never come here
}

bool ExecuteEngine::AddField(TypeId tid, char *val, vector<Field> &fields, ExecuteContext *context) {
  if (val == nullptr)  // null value
  {
    fields.emplace_back(tid, heap_);
  } else if (tid == kTypeInt) {
    fields.emplace_back(kTypeInt, (int32_t)atoi(val), heap_);
  } else if (tid == kTypeFloat)
    fields.emplace_back(kTypeFloat, (float)atof(val), heap_);
  else if (tid == kTypeChar) {
    fields.emplace_back(kTypeChar, val, heap_, strlen(val) + 1, true);
  } else {
    context->output_ += "[Exception]: Invalid field type!\n";
    return false;
  }
  return true;
}

bool ExecuteEngine::RowSatisfyCondition(
    const Row &row, pSyntaxNode cond_root_ast, TableInfo *tinfo,
    ExecuteContext *context)  // judge if a row satisfy the condition generated by the tree
{
  ASSERT(cond_root_ast != nullptr && tinfo != nullptr, "Unexpected null occurs!");

  if (cond_root_ast->type_ == kNodeConditions)  // start of the recursion
  {
    return RowSatisfyCondition(row, cond_root_ast->child_, tinfo, context);
  } else if (cond_root_ast->type_ == kNodeConnector)  // connector
  {
    pSyntaxNode cond = cond_root_ast->child_;
    string connector_str(cond_root_ast->val_);
    // use recursion to examine all conditions
    if (connector_str == "and") {
      while (cond != nullptr) {
        if (!RowSatisfyCondition(row, cond, tinfo, context))  // one condition not satisfied
        {
          return false;
        }
        cond = cond->next_;
      }
      return true;
    } else if (connector_str == "or") {
      while (cond != nullptr) {
        if (RowSatisfyCondition(row, cond, tinfo, context))  // one condition satisfied
        {
          return true;
        }
        cond = cond->next_;
      }
      return false;
    } else
      ASSERT(false, "Unexpected connector!");
  } else if (cond_root_ast->type_ == kNodeCompareOperator)  // leaf of the recursion tree!
  {

    string col_name = cond_root_ast->child_->val_;
    uint32_t col_ind = -1;
    if (tinfo->GetSchema()->GetColumnIndex(col_name, col_ind) == DB_COLUMN_NAME_NOT_EXIST) {
      ASSERT(false, "Get column failed!");
    }
    return CompareSuccess(row.GetField(col_ind), cond_root_ast, cond_root_ast->child_->next_, context);
  } else {
    ASSERT(false, "Unexpected condition node type!");
  }
  return false;  // should not come here
}

string ExecuteEngine::set_width(std::string str, size_t width)
{
  ASSERT(str.size()<=width, "Size error!");
  string ap(width - str.size(),' ');
  str += ap;
  str += " | ";
  return str;
}