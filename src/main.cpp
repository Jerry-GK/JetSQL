#include <cstdio>
#include <time.h>
#include <signal.h>
#include <thread>
#include <mutex>
#include "executor/execute_engine.h"
#include "common/Thread_Share.h"
#include "glog/logging.h"
#include "parser/syntax_tree_printer.h"
#include "utils/tree_file_mgr.h"

//#define ENABLE_PARSER_DEBUG

extern "C" {
  int yyparse(void);
  FILE* yyin;
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

//global variables
vector<string> cmd_history;
extern std::unordered_map<std::string, Thread_Share> global_SharedMap;
extern std::recursive_mutex global_parsetree_latch;
extern std::recursive_mutex global_shared_latch;
extern std::recursive_mutex global_exe_latch;

void generate_shared(); //generate shared sources among threads (buffer pool, disk manager, log manager)
void execption_handle(int sig_num);
void InitGoogleLog(char* argv);
void InputCommand(char* input, const int len);
CommandType PreTreat(char* input);
int run(int&);

int main(int argc, char** argv)
{
  cout << "\nJetSQL initializing shared resources..." << endl;
  generate_shared();

  vector<thread> thread_pool;
  vector<int> tids;
  for (int i = 0; i < THREAD_MAXNUM; i++)
    tids.push_back(i);
  for (int i = 0; i < THREAD_MAXNUM; i++)
  {
    thread_pool.push_back(thread(run, ref(tids[i])));
  }
  for (int i = 0; i < THREAD_MAXNUM; i++)
    thread_pool[i].join();

  return 0;
}

int run(int& thread_id) {
  cout << "\nThread <" << thread_id << "> initializing data..." << endl;
  ExecuteEngine* engine = nullptr;
  if (!USING_LOG)
  {
    //signal handle for non-log mode
    signal(SIGHUP, execption_handle);
    signal(SIGTERM, execption_handle);
    signal(SIGKILL, execption_handle);
    signal(SIGABRT, execption_handle);
    signal(SIGALRM, execption_handle);
    signal(SIGPIPE, execption_handle);
    signal(SIGUSR1, execption_handle);
    signal(SIGUSR2, execption_handle);
    signal(SIGTSTP, execption_handle);
    signal(SIGINT, execption_handle);
    signal(SIGSEGV, execption_handle);
  }

  //InitGoogleLog(argv[0]);
  // command buffer
  const int buf_size = 1024;
  char cmd[buf_size];

  // execute engine
  engine = new ExecuteEngine(DBMETA_FILENAME, thread_id);
  // for print syntax tree
  TreeFileManagers syntax_tree_file_mgr("syntax_tree_");
  //uint32_t syntax_tree_id = 0;

  ExecuteContext context;

  cout << "\nWelcome to use JetSQL!" << endl;
  while (true) {
    cout << "\nThread <" << thread_id << ">: JetSQL > ";

    //flush output
    context.output_.clear();

    // read from buffer
    if (!TEST_CONC)
      InputCommand(cmd, buf_size);
    else //omitted if no concurrency test
    {
      string str;
      if (thread_id == 0)
        str = "execfile \"test-1w-conc0.sql\";";
      else if (thread_id == 1)
        str = "execfile \"test-1w-conc1.sql\";";
      memset(cmd, 0, buf_size);
      memcpy(cmd, str.c_str(), str.size());
    }

    cmd_history.push_back(string(cmd));

    if (PreTreat(cmd) != SQL)
      continue;

    global_parsetree_latch.lock(); //lock parse tree
    // create buffer for sql input
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
      printf("[Parse Error]: %s\n", MinisqlParserGetErrorMessage());
    }
    else {
#ifdef ENABLE_PARSER_DEBUG
      printf("[INFO] Sql syntax parse ok!\n");
      SyntaxTreePrinter printer(MinisqlGetParserRootNode());
      printer.PrintTree(syntax_tree_file_mgr[0]);
#endif
    }

    pSyntaxNode root_node = CopySyntaxTree(MinisqlGetParserRootNode());

    // clean memory after parse
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();
    global_parsetree_latch.unlock(); //unlock parse tree

    context.input_ = cmd;
    clock_t stm_start = clock();
    if (engine->Execute(root_node, &context) != DB_SUCCESS)
    {
      cout << context.output_;
      printf("[Failure]: SQL statement executed failed!\n");
    }
    else
    {
      //count time for a statement
      clock_t stm_end = clock();
      double run_time = (double)((stm_end - stm_start)) / CLOCKS_PER_SEC;
      cout << context.output_;
      printf("[Success]: (run time: %.3f sec)\n", run_time);
    }
    //sleep(1);

    FreeSyntaxTree(root_node);

    if (TEST_CONC)
      context.flag_quit_ = true;

    // quit condition
    if (context.flag_quit_) {
      printf("\n[Quit]: Thanks for using JetSQL, bye!\n");
      break;
    }
  }
  delete engine;
  engine = nullptr;
  return 0;
}

void generate_shared()
{
  std::fstream engine_meta_io;
  engine_meta_io.open(DBMETA_FILENAME, std::ios::in);
  if (!engine_meta_io.is_open()) {
    // create the meta file
    engine_meta_io.open(DBMETA_FILENAME, std::ios::out);
    engine_meta_io.close();
  }
  else
  {
    string db_name;
    while (getline(engine_meta_io, db_name)) {
      if (db_name.empty()) break;
      //try {
      string db_file_name = "../files/db/" + db_name + ".db";
      LogManager* logMgr = nullptr;
      if (USING_LOG)
        logMgr = new LogManager(db_name);
      DiskManager* diskMgr = new DiskManager(db_file_name, logMgr);
      BufferPoolManager* BPMgr = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, diskMgr, logMgr);
      LockManager* lockMgr = nullptr;
      CatalogManager* cataMgr = new CatalogManager(BPMgr, lockMgr, logMgr, false);
      global_SharedMap.insert(make_pair(db_name, Thread_Share(diskMgr, BPMgr, logMgr, cataMgr, lockMgr)));
      // } catch (int) {
      //   cout << "[Exception]: Can not initialize databases meta!\n"
      //           "(Meta file not consistent with db file. May be caused by for forced quit.)"
      //        << endl;
      //   exit(-1);
      // }
    }
    engine_meta_io.close();
  }
}

void execption_handle(int sig_num)
{
  static bool is_quit = false;
  if (is_quit)
  {
    cout << "\n[Exception]: Flush failure during forced quit! (Signal number: " << sig_num << ")" << endl;
    exit(-1);
  }
  cout << "\n[Exception]: Force quit! (Signal number: " << sig_num << ")" << endl;
  is_quit = true;

  //delete engine;
  exit(-1);
}

void InitGoogleLog(char* argv)
{
  FLAGS_logtostderr = true;
  FLAGS_colorlogtostderr = true;
  google::InitGoogleLogging(argv);
}

void InputCommand(char* input, const int len)
{
  memset(input, 0, len);
  int i = 0;
  char ch;
  while ((ch = getchar()) != ';') {
    input[i++] = ch;
  }
  input[i] = ch;    // ;
  getchar();        // remove enter
}

CommandType PreTreat(char* input)
{
  //pretreat special commands
  //cancel unnecessary spaces
  //...

  if (*input == '\0')
    return EMPTY;
  string cmd(input);
  if (cmd[0] == '-')
  {
    if (cmd == "-clear;")
    {
      system("clear");
    }
    // else if(cmd == "-set mode FAST;")
    // {
    //   cout<<"[Warning]: Strongly not recommended to change mode during running!"<<endl;
    //   CUR_DBMS_MODE = FAST;
    //   static bool USING_LOG = (CUR_DBMS_MODE!=FAST);
    //   cout<<"[Setting]: Mode changed to FAST"<<endl;
    // }
    // else if(cmd == "-set mode SAFE;")
    // {
    //   cout<<"[Warning]: Strongly not recommended to change mode during running!"<<endl;
    //   CUR_DBMS_MODE = SAFE;
    //   static bool USING_LOG = (CUR_DBMS_MODE!=FAST);
    //   cout<<"[Setting]: Mode changed to SAFE"<<endl;
    // }
    // else if(cmd == "-set index BPTREE;")
    // {
    //   cout<<"[Warning]: Strongly not recommended to change index type during running!"<<endl;
    //   DEFAULT_INDEX_TYPE = BPTREE;
    //   cout<<"[Setting]: Index type changed to BPTREE"<<endl;
    // }
    // else if(cmd == "-set index HASH;")
    // {
    //   cout<<"[Warning]: Strongly not recommended to change index type during running!"<<endl;
    //   DEFAULT_INDEX_TYPE = HASH;
    //   cout<<"[Setting]: Index type changed to HASH"<<endl;
    // }
    else if (cmd == "-help;")
    {
      cout << "SQL command: Surpport basic SQL. Please refer to ReadMe.md" << endl;
      cout << "System command (begin with '-', end with ';'): \n"
        "-clear; : clear the screen\n"
        "-help; : check help message\n" << endl;
    }
    else
    {
      cout << "[Error]: Unknown system command \"" + cmd + "\" !" << endl;
    }
    return SYSTEM;
  }
  return SQL;
}