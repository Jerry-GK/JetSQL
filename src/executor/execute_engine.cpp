#include "executor/execute_engine.h"
#include "glog/logging.h"

//#define ENABLE_EXECUTE_DEBUG
ExecuteEngine::ExecuteEngine(string engine_meta_file_name) {
  //get existed database from meta file
  engine_meta_file_name_ = engine_meta_file_name;
  engine_meta_io_.open(engine_meta_file_name_, std::ios::in);
  if(!engine_meta_io_.is_open())
  {
    //create the meta file
    engine_meta_io_.open(engine_meta_file_name_, std::ios::out);
    engine_meta_io_.close();
    return;
  }
  string db_name;
  while(getline(engine_meta_io_, db_name))
  {
    if(db_name.empty())
      break;
    DBStorageEngine* new_engine = new DBStorageEngine(db_name + ".db", false);//load a existed database
    dbs_.insert(make_pair(db_name, new_engine));
  }
  engine_meta_io_.close();
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
  string db_name = ast->child_->val_;
  if(dbs_.find(db_name)!=dbs_.end())//database already exists
  {
    cout << "Error: Database \"" << db_name << "\" already exists!" << endl;
    return DB_FAILED;
  }
  DBStorageEngine* new_engine = new DBStorageEngine(db_name + ".db");//create a new database
  dbs_.insert(make_pair(db_name, new_engine));
  engine_meta_io_.open(engine_meta_file_name_, std::ios::app);

  ASSERT(engine_meta_io_.is_open(), "No meta file!");
  engine_meta_io_<<db_name<<endl;//add a line
  engine_meta_io_.close();

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

  delete dbs_.find(db_file_name)->second;

  //dbs_.find(db_file_name)->second->delete_file();
  delete dbs_.find(db_file_name)->second;
  dbs_.erase(db_file_name);
  if(db_file_name==current_db_)
    current_db_="";


  //clear the meta file and rewrite all remained db names (for convinience, difficult to delete a certain line)
  engine_meta_io_.open(engine_meta_file_name_, std::ios::out);
  ASSERT(engine_meta_io_.is_open(), "No meta file!");
  //rewrite the meta file
  for(unordered_map<std::string, DBStorageEngine *>::iterator it = dbs_.begin(); it != dbs_.end(); it++)
  {
    engine_meta_io_ << it->first << endl;
  }
  engine_meta_io_.close();

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
  for (unordered_map<std::string, DBStorageEngine *>::iterator it = dbs_.begin(); it != dbs_.end(); it++) 
  {
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
  TableInfo* tinfo_temp;
  if(dbs_[current_db_]->catalog_mgr_->GetTable(table_name, tinfo_temp)==DB_SUCCESS)
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
      columns.push_back(ALLOC_COLUMN(heap)(coloumn_name, tid, char_length, index, true, is_unique));//always nullable
    else
      columns.push_back(ALLOC_COLUMN(heap)(coloumn_name, tid, index, true, is_unique));
    //cout << "name = "<< coloumn_name << " tid = " << tid << " index = " << index << " len = " << char_length
         //<< " is_unique = " << is_unique << endl;
    ast = ast->next_;
    index++;
  }

  auto schema = std::make_shared<Schema>(columns);
  TableInfo *tinfo;//not uesed?

  dberr_t ret = dbs_[current_db_]->catalog_mgr_->CreateTable(table_name, schema.get(), nullptr, tinfo);//create table

  //create index on primary key if exists
  if(ast!=nullptr)
  {
    ASSERT(ast->type_ == kNodeColumnList, "table syntax tree error!");
    string decl_str = ast->val_;
    ASSERT(decl_str== "primary keys", "not primary key declaration!");

    //create index on primary key
    vector<string> pri_index_keys;
    string pri_index_name = "_AUTO_PRI_"+table_name+"_";
    pSyntaxNode p_index = ast->child_;
    while(p_index!=nullptr)
    {
      pri_index_keys.push_back(p_index->val_);
      pri_index_name += p_index->val_;
      pri_index_name += "_";
      p_index = p_index->next_;
    }
    
    IndexInfo *iinfo;
    if(dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, pri_index_name, pri_index_keys, nullptr, iinfo)!=DB_SUCCESS)
      return DB_FAILED;
  }

  //create index on unqiue key if exists
  for(auto col : tinfo->GetSchema()->GetColumns())
  {
    if(col->IsUnique())//create auto index
    {
      IndexInfo *iinfo;
      string unique_key = col->GetName();
      string unique_index_name = "_AUTO_UNIQUE_" + table_name + "_" + unique_key + "_";
      vector<string> unique_key_vec;
      unique_key_vec.push_back(unique_key);
      if (dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, unique_index_name, unique_key_vec, nullptr, iinfo) !=DB_SUCCESS)
        return DB_FAILED;
    }
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
        cout << (*itt)->GetIndexName() << "  ";
    }
    cout << endl;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  //step 1: get the table
  if(current_db_=="")
  {
    cout << "Error: No database used!" << endl;
    return DB_FAILED;
  }

  //step 2: examine the index naming
  string table_name = ast->child_->next_->val_;
  string index_name = ast->child_->val_;
  if(index_name.find("_AUTO")==0)
  {
    cout<<"Error: Do not name your index in AUTO index naming formula!"<<endl;
    return DB_FAILED;
  }

  //step 3: generate the index key names
  pSyntaxNode keys_root = ast->child_->next_->next_;
  vector<string> index_keys;
  pSyntaxNode key = keys_root->child_;
  while(key!=nullptr)
  {
    index_keys.push_back(key->val_);
    key = key->next_;
  }

  //step 4: check table and index existence
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

  //step 5: check the unique constraint
  TableInfo* tinfo;
  dbs_[current_db_]->catalog_mgr_->GetTable(table_name, tinfo);
  if(index_keys.size()>1)//not implement multiple uniqueness check, so no multiple non-primary index
  {
    cout << "Error: Multiple non-primary unique key index is not available yet!" << endl;
    return DB_FAILED;
  }
  else
  {
    string index_key_name = index_keys[0];
    for(auto col : tinfo->GetSchema()->GetColumns())//can not get col by name yet
    {
      if(col->GetName() == index_key_name)
      {
        if(col->IsUnique())//match
          break;
        else
        {
          cout<<"Error: Can not create index on non-unique field!"<<endl;
          return DB_FAILED;
        }
      }
    }
  }

  //step 6: create the index
  if(dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, index_name, index_keys, nullptr, iinfo)!=DB_SUCCESS)
    return DB_FAILED;

  //step 7: Initialization:
  //after create a nex index on a table, we have to insert initial entries into the index if the table is not empty!
  TableHeap* table_heap = tinfo->GetTableHeap();

  for(auto it = table_heap->Begin(); it != table_heap->End(); it++)
  {
    Row row = *it;
    // generate the inserted key
    vector<Field> key_fields;
    for(uint32_t i=0;i<iinfo->GetIndexKeySchema()->GetColumnCount();i++)
    {
      key_fields.push_back(*row.GetField(iinfo->GetIndexKeySchema()->GetColumn(i)->GetTableInd()));
    }
    Row key(key_fields);
    key.SetRowId(row.GetRowId());//key rowId is the same as the inserted row

    // do insert entry
    if(iinfo->GetIndex()->InsertEntry(key, key.GetRowId(), nullptr)!=DB_SUCCESS)
    {
      cout<<"Error: Initialize index failed while doing create index on a non-emtpy table (may exist duplicate keys)!"<<endl;
      return DB_FAILED;
    }
  }
  return DB_SUCCESS;
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
  if(index_name.find("_AUTO")==0)
  {
    cout<<"Error: Can not drop index of an AUTO key!"<<endl;
    return DB_FAILED;
  }
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
  //step 1: get table
  if(current_db_=="")
  {
    cout << "Error: No database used!" << endl;
    return DB_FAILED;
  }

  string table_name = ast->child_->next_->val_;
  TableInfo *tinfo;
  if(dbs_[current_db_]->catalog_mgr_->GetTable(table_name, tinfo)!=DB_SUCCESS)
  {
    cout << "Error: Table \"" << table_name << "\" not exists!" << endl;
    return DB_TABLE_NOT_EXIST;
  }

  vector<IndexInfo*> iinfos;
  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, iinfos);

  //step 2: do the selection
  vector<Row> rows;
  if(SelectTuples(ast->child_->next_->next_, context,tinfo, iinfos, &rows)!=DB_SUCCESS)//critical function
  {
    cout<<"Error: Tuple selected failed!"<<endl;
    return DB_FAILED;
  }

  //step 3: do the projection
  vector<Row> selected_rows;
  if(ast->child_->type_==kNodeAllColumns)
  {
    for(auto row:rows)
      selected_rows.push_back(row);
  }
  else//project each row
  {
    ASSERT(ast->child_->type_==kNodeColumnList,"No column list for projection");
    Schema* sch = tinfo->GetSchema();
    for(auto row : rows)
    {
      vector<Field> selected_row_fields;
      pSyntaxNode p_col = ast->child_->child_;
      while(p_col!=nullptr)
      {
        ASSERT(p_col->type_==kNodeIdentifier,"No column identifier");
        string col_name = p_col->val_;
        uint32_t col_index;
        if(sch->GetColumnIndex(col_name, col_index)==DB_COLUMN_NAME_NOT_EXIST)
        {
          cout << "Error: Column \""<<col_name<<"\" not exists!" << endl;
          return DB_COLUMN_NAME_NOT_EXIST;
        }
        selected_row_fields.push_back(*row.GetField(col_index));
        p_col=p_col->next_;
      }

      Row selected_row(selected_row_fields);
      selected_rows.push_back(selected_row);
    }
  }

  //step 4: do the output
  //output the table name and the selected column name
  cout << "Table: " << table_name << endl;
  if(ast->child_->type_==kNodeAllColumns)
  {
    for(uint32_t i=0; i<tinfo->GetSchema()->GetColumnCount();i++)
      cout << tinfo->GetSchema()->GetColumn(i)->GetName() << "  ";
  }
  else
  {
    pSyntaxNode p_col = ast->child_->child_;
    while(p_col!=nullptr)
    {
      ASSERT(p_col->type_==kNodeIdentifier,"No column identifier");
      cout << p_col->val_ << "  ";
      p_col = p_col->next_;
    }
  }
  cout<<endl;

  //out put the rows
  uint32_t col_num = 0;
  for (vector<Row>::iterator it = selected_rows.begin(); it != selected_rows.end(); it++) {   
    vector<Field *> fields = it->GetFields();
    for (vector<Field *>::iterator itt = fields.begin(); itt != fields.end(); itt++) {
      if((*itt)->IsNull())
        cout<<"null"<<"  ";
      else//do output
        cout<<(*itt)->GetData()<<"  "; 
    }
    cout << endl;
    col_num++;
  }
  cout << "(" << col_num << " rows in set)"<<endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  if(current_db_=="")
  {
    cout << "Error: No database used!" << endl;
    return DB_FAILED;
  }

  //step 1: get the table
  string table_name;
  table_name = ast->child_->val_;
  TableInfo *tinfo;
  if(dbs_[current_db_]->catalog_mgr_->GetTable(table_name,tinfo)!=DB_SUCCESS)
  {
    cout<<"Error: Table \""<<table_name<<"\" not exists!"<<endl;
    return DB_TABLE_NOT_EXIST;
  }

  //step 2: generate the inserted row
  Schema* sch = tinfo->GetSchema();//get schema
  pSyntaxNode p_value = ast->child_->next_->child_;
  vector<Field> fields;//fields in a row to be inserted
  uint32_t col_num = 0;
  while(p_value!=nullptr)
  { 
    AddField(sch->GetColumn(col_num)->GetType(), p_value->val_, fields);
    col_num++;
    p_value = p_value->next_;
  }
  if(col_num != sch->GetColumnCount())
  {
    cout<<"Error: Inserted field number not matched!"<<endl;
    return DB_FAILED;
  }
  Row row(fields);//the row waiting to be inserted

  //step 3: check the index->unique constraint (return DB_FAILED if constraint vialation happens)
  vector<IndexInfo*> iinfos;
  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, iinfos);
  if(tinfo->GetTableHeap()->Begin()!=tinfo->GetTableHeap()->End())//not empty table, avoid scankey bug of empty index
  {
    for(vector<IndexInfo*>::iterator it = iinfos.begin(); it!=iinfos.end();it++)//traverse every index on the table
    {
      //generate the inserted key
      vector<Field> key_fields;
      for(uint32_t i=0;i<(*it)->GetIndexKeySchema()->GetColumnCount();i++)
      {
        key_fields.push_back(*row.GetField((*it)->GetIndexKeySchema()->GetColumn(i)->GetTableInd()));
      }

      Row key(key_fields);
      key.SetRowId(row.GetRowId());//key rowId is the same as the inserted row

      // check if violate unique constraint
      vector<RowId> temp;
      if((*it)->GetIndex()->ScanKey(key, temp, nullptr)!=DB_KEY_NOT_FOUND)
      {
        cout << "Error: Inserted row has caused duplicate values in the table against index \""<<(*it)->GetIndexName()<<"\"!";
        return DB_FAILED;
      }
    }
  }
  
  /*
  //check unique constraint
  for(auto col : sch->GetColumns())
  {
    if(col->IsUnique())
    {
      string theo_index_name;//theoratically index name
      theo_index_name = "_AUTO_UNIQUE_"+table_name+"_" + col->GetName() + "_";
      IndexInfo* iinfo;
      if(dbs_[current_db_]->catalog_mgr_->GetIndex(table_name, theo_index_name, iinfo)!=DB_SUCCESS)
      {
        cout << "Error <fatal>: No index for uniqie key \""<<col->GetName()<<"\"!";
        return DB_FAILED;
      }
      vector<Field> unique_fields;
      Field key_field(fields[col->GetTableInd()]);
      unique_fields.push_back(key_field);
      Row unique_key_row(unique_fields);  // get the inserted value of the unique key
      vector<RowId> temp_v;
      if((iinfo->GetIndex()->ScanKey(unique_key_row, temp_v, nullptr))!=DB_KEY_NOT_FOUND)
      {
        cout << "Error: Insert row has duplicate values against unique key \"" << col->GetName() << "\"!";
        return DB_FAILED;
      }
    }
  }
  */

  //step 4: do the insertion (insert tuple + insert each related index )
  iinfos.clear();
  if (tinfo->GetTableHeap()->InsertTuple(row, nullptr))  // insert the tuple, rowId has been set
  {
    //update index(do not forget!)
    dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, iinfos);
    for(vector<IndexInfo*>::iterator it = iinfos.begin(); it!=iinfos.end();it++)//traverse every index on the table
    {
      //generate the inserted key
      vector<Field> key_fields;
      for(uint32_t i=0;i<(*it)->GetIndexKeySchema()->GetColumnCount();i++)
      {
        key_fields.push_back(*row.GetField((*it)->GetIndexKeySchema()->GetColumn(i)->GetTableInd()));
      }

      Row key(key_fields);
      key.SetRowId(row.GetRowId());//key rowId is the same as the inserted row

      // do insert entry into the index
      if((*it)->GetIndex()->InsertEntry(key, key.GetRowId(), nullptr)!=DB_SUCCESS)
      {
        cout<<"Error <fatal>: Insert index entry failed while doing update (data may be inconsistent)!"<<endl;
        return DB_FAILED;
      }
    }
    return DB_SUCCESS;
  }
  cout<<"Error: Insert failed!"<<endl;
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  //step 1: get table
  if(current_db_=="")
  {
    cout << "Error: No database used!" << endl;
    return DB_FAILED;
  }
  string table_name = ast->child_->val_;
  TableInfo *tinfo;
  if(dbs_[current_db_]->catalog_mgr_->GetTable(table_name, tinfo)!=DB_SUCCESS)
  {
    cout << "Error: Table \"" << table_name << "\" not exists!" << endl;
    return DB_TABLE_NOT_EXIST;
  }
  vector<IndexInfo*> iinfos;
  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, iinfos);

  //step 2: do the row selection
  vector<Row> rows;
  if(SelectTuples(ast->child_->next_, context, tinfo,iinfos, &rows)!=DB_SUCCESS)//critical function
  {
    cout<<"Error: Tuple selected failed!"<<endl;
    return DB_FAILED;
  }
  
  //step 3: delete the rows
  for(auto row : rows)
  {
    if(tinfo->GetTableHeap()->MarkDelete(row.GetRowId(), nullptr))//mark delete the tuple, rowId has been set
    {
      //update index(do not forget!)
      for(vector<IndexInfo*>::iterator it = iinfos.begin(); it!=iinfos.end();it++)//traverse every index on the table
      {
        // do remove entry
        vector<Field> key_fields;
        for(uint32_t i=0;i<(*it)->GetIndexKeySchema()->GetColumnCount();i++)
        {
          key_fields.push_back(*row.GetField((*it)->GetIndexKeySchema()->GetColumn(i)->GetTableInd()));
        }
        Row key(key_fields);
        key.SetRowId(row.GetRowId());//key rowId is the same as the inserted row

        if((*it)->GetIndex()->RemoveEntry(key, key.GetRowId(), nullptr)!=DB_SUCCESS)
        {
          cout<<"Error: Remove index failed while doing deletion!"<<endl;
          return DB_FAILED;
        }
      }
    }
    else
    {
      cout<<"Error: Mark delete tuple failed!"<<endl;
      return DB_FAILED;
    }
    tinfo->GetTableHeap()->ApplyDelete(row.GetRowId(), nullptr);
    //if apply delete failed
    // {
    //   cout<<"Error: Apply delete tuple failed!"<<endl;
    //   return DB_FAILED;
    // }
  }
  cout<<"("<<rows.size()<<" rows deleted)"<<endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  //step 1: get the table
  if(current_db_=="")
  {
    cout << "Error: No database used!" << endl;
    return DB_FAILED;
  }
  string table_name = ast->child_->val_;
  TableInfo *tinfo;
  if(dbs_[current_db_]->catalog_mgr_->GetTable(table_name, tinfo)!=DB_SUCCESS)
  {
    cout << "Error: Table \"" << table_name << "\" not exists!" << endl;
    return DB_TABLE_NOT_EXIST;
  }
  Schema* sch = tinfo->GetSchema();
  vector<IndexInfo*> iinfos;
  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, iinfos);

  //step 2: do the row selection
  vector<Row> rows;
  if(SelectTuples(ast->child_->next_->next_, context, tinfo, iinfos, &rows)!=DB_SUCCESS)//critical function
  {
    cout<<"Error: Tuple selected failed!"<<endl;
    return DB_FAILED;
  }

  //step 3: save the column name and update value
  unordered_map<string, pSyntaxNode> update_cols;
  pSyntaxNode p_cols = ast->child_->next_;
  ASSERT(p_cols->type_ == kNodeUpdateValues, "Wrong node for update!");
  p_cols = p_cols->child_;
  ASSERT(p_cols->type_ == kNodeUpdateValue, "Wrong node for update!");
  while(p_cols!=nullptr)
  {
    update_cols.insert(make_pair(p_cols->child_->val_, p_cols->child_->next_));
    uint32_t temp_ind;
    if(sch->GetColumnIndex(p_cols->child_->val_, temp_ind)==DB_COLUMN_NAME_NOT_EXIST)
    {
      cout << "Error: Column \""<<p_cols->child_->val_<<"\" not exists!" << endl;
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    p_cols = p_cols->next_;
  }

  //step 4: generate the updated rows
  vector<Row> new_rows;
  for(auto row : rows)
  {
    vector<Field*> old_fields_p = row.GetFields();
    vector<Field> new_fields;
    int col_index = 0;
    for (auto field_p : old_fields_p) 
    {
      unordered_map<string, pSyntaxNode>::iterator it = update_cols.find(sch->GetColumn(col_index)->GetName());
      if(it != update_cols.end())//update the field
      {
        //update field
        AddField(sch->GetColumn(col_index)->GetType(), it->second->val_, new_fields);
      }
      else//copy the original field
      {
        new_fields.push_back(*field_p);
      }
      col_index++;
    }
    Row new_row(new_fields);
    new_row.SetRowId(row.GetRowId());
    new_rows.push_back(new_row);
  }

  //step 5: check(every updated new row) the index->unique constraint (return DB_FAILED if constraint vialation happens)
  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, iinfos);
  for(auto new_row : new_rows)
  {
    if(tinfo->GetTableHeap()->Begin()!=tinfo->GetTableHeap()->End())//not empty table, avoid scankey bug of empty index
    {
      for(vector<IndexInfo*>::iterator it = iinfos.begin(); it!=iinfos.end();it++)//traverse every index on the table
      {
        //generate the inserted key and check the intersaction at the same time
        vector<Field> key_fields;
        bool have_intersact=false;//if the key of the checked index has common columns with the new row. if not, no need to check  
        for(uint32_t i=0;i<(*it)->GetIndexKeySchema()->GetColumnCount();i++)
        {
          for(auto update_col:update_cols)
          {
            if(update_col.first==(*it)->GetIndexKeySchema()->GetColumn(i)->GetName())
            {
              have_intersact = true;
            }
          }
          key_fields.push_back(*new_row.GetField((*it)->GetIndexKeySchema()->GetColumn(i)->GetTableInd()));
        }
        if(!have_intersact)
          continue;

        Row key(key_fields);
        key.SetRowId(new_row.GetRowId());//key rowId is the same as the inserted row

        // check if violate unique constraint
        vector<RowId> temp;
        if((*it)->GetIndex()->ScanKey(key, temp, nullptr)!=DB_KEY_NOT_FOUND)
        {
          cout << "Error: Inserted row has caused duplicate values in the table against index \""<<(*it)->GetIndexName()<<"\"!";
          return DB_FAILED;
        }
      }
    }
  }

  // step 6: update (update index entry as well)
  ASSERT(new_rows.size() == rows.size(), "Rows number not matched!");
  for (uint32_t i =0;i<new_rows.size();i++)  // update every selected row
  {
    Row new_row = new_rows[i];
    Row old_row = rows[i];
    // update the row with new row
    if(!tinfo->GetTableHeap()->UpdateTuple(new_row, old_row.GetRowId(), nullptr))
    {
      cout<<"Error: Can not update tuple!"<<endl;
      return DB_FAILED;
    }

    //update index entry
    for(vector<IndexInfo*>::iterator it = iinfos.begin(); it!=iinfos.end();it++)//traverse every index on the table
    {
      //remove the old entry
      vector<Field> old_key_fields;
      for(uint32_t i=0;i<(*it)->GetIndexKeySchema()->GetColumnCount();i++)
      {
        old_key_fields.push_back(*old_row.GetField((*it)->GetIndexKeySchema()->GetColumn(i)->GetTableInd()));
      }
      Row old_key(old_key_fields);
      old_key.SetRowId(old_row.GetRowId());
      if((*it)->GetIndex()->RemoveEntry(old_key, old_key.GetRowId(),nullptr)!=DB_SUCCESS)
      {
        cout<<"Error: Remove index failed while doing update (may exist duplicate keys)!"<<endl;
        return DB_FAILED;
      }
      vector<Field> new_key_fields;
      for(uint32_t i=0;i<(*it)->GetIndexKeySchema()->GetColumnCount();i++)
      {
        new_key_fields.push_back(*new_row.GetField((*it)->GetIndexKeySchema()->GetColumn(i)->GetTableInd()));
      }
      Row new_key(new_key_fields);
      new_key.SetRowId(new_row.GetRowId());

      // do insert new entry
      if((*it)->GetIndex()->InsertEntry(new_key, new_key.GetRowId(), nullptr)!=DB_SUCCESS)//why failed(duplicate)?
      {
        cout<<"Error <fatal>: Insert index entry failed while doing update (data may be inconsistent)!"<<endl;
        return DB_FAILED;
      }
    }
  }
  cout<<"("<<new_rows.size()<<" rows updated)"<<endl;
  return DB_SUCCESS;  
}

//--------------------------------Transaction(not implemented yet)--------------------------------------------------
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
  string sql_file_name = ast->child_->val_;
  fstream sql_file_io;
  sql_file_io.open(sql_file_name, ios::in);
  if(!sql_file_io.is_open())
  {
    cout<<"Error: Can not open SQL file \""<<sql_file_name<<"\"!"<<endl;
    return DB_FAILED;
  }

  const int buf_size = 1024;
  char cmd[buf_size];
  string cmd_str;
  while (getline(sql_file_io, cmd_str)) 
  {
    cout << "execute: " <<cmd_str << endl;
    for (size_t i = 0; i < cmd_str.size(); i++) cmd[i] = cmd_str[i];
    if(cmd_str[cmd_str.size()-1]!=';')
    {
      cout << "Error: SQL statements in each line must end by \";\" !";
      sql_file_io.close();
      return DB_FAILED;
    }
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
    
    if (MinisqlParserGetError()) {
      // error
      printf("%s\n", MinisqlParserGetErrorMessage());
    } else {
      #ifdef ENABLE_PARSER_DEBUG
            printf("[INFO] Sql syntax parse ok!\n");
            SyntaxTreePrinter printer(MinisqlGetParserRootNode());
            printer.PrintTree(syntax_tree_file_mgr[0]);
      #endif
      }
      
      Execute(MinisqlGetParserRootNode(), context);
      cout << endl;
      // clean memory after parse
      MinisqlParserFinish();
      yy_delete_buffer(bp);
      yylex_destroy();

      // quit condition
      if (context->flag_quit_) {
        break;
      }
  }
  //dbs_[current_db_]->bpm_->get_hit_rate();
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


//my added member function (critical part)
//cond_root_ast: the root node for the Condition Node in syntax tree
//tinfo: the current selected table info
//iinfos: the indexes info of current table
//rows: receive the result
dberr_t ExecuteEngine::SelectTuples(const pSyntaxNode cond_root_ast, ExecuteContext *context, 
                                    TableInfo* tinfo, vector<IndexInfo*> iinfos, vector<Row>* rows)//select the rows according to the condition node
{
  ASSERT(tinfo!=nullptr, "Null for select");
  ASSERT(cond_root_ast==nullptr||cond_root_ast->type_ == kNodeConditions, "No condition nodes!");

  TableHeap *table_heap = tinfo->GetTableHeap();

  //do selection (traverse and return all now! need to implement)
  if(cond_root_ast==nullptr)//no condition(return all tuples)
  {
    for (auto it = table_heap->Begin(); it != table_heap->End(); it++)  // traverse tuples
    {
      rows->push_back(*it);
    }
  }
  else if(cond_root_ast->child_->type_==kNodeCompareOperator)//single condition
  {
    string col_name = cond_root_ast->child_->child_->val_;
    uint32_t col_ind=-1;
    if(tinfo->GetSchema()->GetColumnIndex(col_name, col_ind)==DB_COLUMN_NAME_NOT_EXIST)
    {
      cout << "Error: Column \""<<col_name<<"\" not exists!" << endl;
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    //check if there is an index to use
    bool have_index = false;
    for(auto iinfo:iinfos)
    {
      if(iinfo->GetIndexKeySchema()->GetColumnCount()==1 && iinfo->GetIndexKeySchema()->GetColumn(0)->GetName()==col_name)
      {
        // found an possible index,
        vector<Field> fields;
        AddField(iinfo->GetIndexKeySchema()->GetColumn(0)->GetType(), cond_root_ast->child_->child_->next_->val_, fields);
        //no consider for null insertion for index column now!

        Row key(fields);
        vector<RowId> select_rid;
        iinfo->GetIndex()->ScanKey(key, select_rid, nullptr);
        if(select_rid.empty())//search value is not in index keys
        {
          continue;//can not use this index to search because key does not exists
        }

        //find the feasible index
        have_index = true;
        cout << "Using index \""<<iinfo->GetIndexName()<<"\" to query!" << endl;
        auto target = tinfo->GetTableHeap()->Find(select_rid[0]);//target is the table iterator for target value

        string comp_str(cond_root_ast->child_->val_);
        if (comp_str == "=") 
        {
          rows->push_back(*target);
        } 
        else if (comp_str == "!=") 
        {
          for (auto it = table_heap->Begin(); it != table_heap->End(); it++)  // return all but not target
          {
            if(it!=target)
              rows->push_back(*it);
          }
        } 
        else if (comp_str == ">")
        {
          for (auto it = target; it != table_heap->End(); it++)  // return all larger than target
          {
            if(it==target)
              continue;
            rows->push_back(*it);
          }
        } 
        else if (comp_str == ">=") 
        {
          for (auto it = target; it != table_heap->End(); it++)  // return all >= target
          {
            rows->push_back(*it);
          }
        } 
        else if (comp_str == "<") 
        {
          for (auto it = table_heap->Begin(); it != target; it++)  // return all less than target
          {
            rows->push_back(*it);
          }
        } 
        else if (comp_str == "<=") 
        {
          for (auto it = table_heap->Begin(); ; it++)  // return all <= target
          {
            rows->push_back(*it);
            if (it == target)
              break;
          }
        } 
        else
          ASSERT(true, "Invalid comparator!");
        break;//won't come here
      }
    }
    //no available index on single condition column, traverse and examine
    if(!have_index)
    {
      for (auto it = table_heap->Begin(); it != table_heap->End(); it++)  // traverse tuples
      {
        //check the comparasion
        if(CompareSuccess((*it).GetField(col_ind), cond_root_ast->child_, cond_root_ast->child_->child_->next_))
          rows->push_back(*it);
      }
    }
  }
  else if(cond_root_ast->child_->type_==kNodeConnector)//multiple condition
  {
    ASSERT(true, "Multiple select condition not implemented yet!");
    //to be implemented
  }
  else
    ASSERT(true, "Unknown select condition!");
  return DB_SUCCESS;
}

//single comparison function, return true if comparision pass
//f: the field
//p_comp:the comparator (=, !=, >, <, <=, >=, is, not)
bool ExecuteEngine::CompareSuccess(Field* f, pSyntaxNode p_comp, pSyntaxNode p_val)
{
  ASSERT(f!=nullptr&&p_comp!=nullptr&&p_val!=nullptr, "Compaision failed!");
  string comp_str(p_comp->val_);
  //first check the null condition
  if(comp_str=="is")
  {
    ASSERT(p_val->type_==kNodeNull, "Compare operator is invalid"); 
    return f->IsNull();
  }
  else if(comp_str=="not")
  {
    ASSERT(p_val->type_==kNodeNull, "Compare operator is invalid"); 
    return !f->IsNull();
  }

  //generate the compared field
  vector<Field> right_f_;
  AddField(f->GetTypeId(), p_val->val_, right_f_);
  ASSERT(!right_f_.empty(), "No right field");

  Field right_f(right_f_[0]);//because 

  if (comp_str == "=") 
  {
    return f->CompareEquals(right_f);
  } 
  else if (comp_str == "!=") 
  {
    return f->CompareNotEquals(right_f);
  } 
  else if (comp_str == ">")
  {
    return f->CompareGreaterThan(right_f);
  } 
  else if (comp_str == ">=") 
  {
    return f->CompareGreaterThanEquals(right_f);
  } 
  else if (comp_str == "<") 
  {
    return f->CompareLessThan(right_f);
  } 
  else if (comp_str == "<=") 
  {
    return f->CompareLessThanEquals(right_f);
  } 
  else
    ASSERT(true, "Invalid comparator!");

  return false;//should never come here
}

 bool ExecuteEngine::AddField(TypeId tid, char* val, vector<Field>& fields)
 {
    if(val==nullptr)//null value
    {
      fields.push_back(Field(tid));
    }
    else if (tid==kTypeInt)
      fields.push_back(Field(kTypeInt, (int32_t)atoi(val)));
    else if (tid == kTypeFloat)
      fields.push_back(Field(kTypeFloat, (float)atof(val)));
    else if (tid == kTypeChar)
    {
      fields.push_back(Field(kTypeChar, val, strlen(val) + 1, true));
    }
    else
    {
      ASSERT(true, "Invalid field type!");
      return false;
    }
    return true;
 }