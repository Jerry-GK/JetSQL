#include <cstdio>
#include <time.h>
#include <signal.h>
#include "executor/execute_engine.h"
#include "glog/logging.h"
#include "parser/syntax_tree_printer.h"
#include "utils/tree_file_mgr.h"

//#define ENABLE_PARSER_DEBUG

extern "C" {
int yyparse(void);
FILE *yyin;
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

ExecuteEngine *engine = nullptr;

vector<string> cmd_history;

enum CommandType {SQL, EMPTY, SYSTEM};

void quit_flush(int sig_num)
{
  static bool is_quit = false;
  if(is_quit)
  {
    cout<<"\n[Exception]: Flush failure during forced quit! (Signal number: " << sig_num <<")"<<endl;
    exit(-1);
  }
  cout<<"\n[Exception]: Force quit! (Signal number: " << sig_num <<")"<<endl;
  is_quit = true;
  
  delete engine;
  exit(-1);
}

void InitGoogleLog(char *argv) {
  FLAGS_logtostderr = true;
  FLAGS_colorlogtostderr = true;
  google::InitGoogleLogging(argv);
}

void InputCommand(char *input, const int len) {
  memset(input, 0, len);
  printf("\nminisql > ");
  int i = 0;
  char ch;
  while ((ch = getchar()) != ';') {
    // if(ch=='-')
    // {
    //   string last_cmd;
    //   if(!cmd_history.empty())
    //   {
    //     last_cmd = cmd_history.back();
    //   }
    //   cout<<"\nminisql > "<<last_cmd<<endl;
    //   memcpy(input, last_cmd.c_str(), last_cmd.length()+1);
    // }
    // else
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

  if(*input=='\0')
    return EMPTY;
  string cmd(input);
  if(cmd[0]=='-')
  {
    if(cmd == "-clear;")
    {
      system("clear");
    }
    else if(cmd == "-help;")
    {
      cout<<"Help page to be writen!"<<endl;
    }
    else
    {
      cout<<"[Error]: Unknown system command \"" + cmd + "\" !"<<endl;
    }
    return SYSTEM;
  }

  return SQL;
}

int main(int argc, char **argv) {
  
  signal(SIGHUP, quit_flush);
  signal(SIGTERM, quit_flush);
  signal(SIGKILL, quit_flush);
  signal(SIGABRT, quit_flush);
  signal(SIGALRM, quit_flush);
  signal(SIGPIPE, quit_flush);
  signal(SIGUSR1, quit_flush);
  signal(SIGUSR2, quit_flush);
  signal(SIGTSTP, quit_flush);
  signal(SIGSEGV, quit_flush);

  InitGoogleLog(argv[0]);
  // command buffer
  const int buf_size = 1024;
  char cmd[buf_size];

  string engine_meta_file_name = "../doc/meta/DatabaseMeta.txt";
  // execute engine
  engine = new ExecuteEngine(engine_meta_file_name);
  // for print syntax tree
  TreeFileManagers syntax_tree_file_mgr("syntax_tree_");
   //uint32_t syntax_tree_id = 0;

  ExecuteContext context;


  while (1) {
    //flush output
    context.output_.clear();
    
    // read from buffer
    InputCommand(cmd, buf_size);

    cmd_history.push_back(string(cmd));

    if(PreTreat(cmd)!=SQL)
      continue;
    
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
    } else {
#ifdef ENABLE_PARSER_DEBUG
      printf("[INFO] Sql syntax parse ok!\n");
      SyntaxTreePrinter printer(MinisqlGetParserRootNode());
      printer.PrintTree(syntax_tree_file_mgr[0]);
#endif
    }

    context.input_ = cmd;
    clock_t stm_start = clock();
    if(engine->Execute(MinisqlGetParserRootNode(), &context)!=DB_SUCCESS)
    {
      cout << context.output_;
      printf("[Failure]: SQL statement executed failed!\n");
    }
    else
    {
      //count time for a statement
      clock_t stm_end = clock();
      double run_time = (double)((stm_end - stm_start))/CLOCKS_PER_SEC;
      cout << context.output_;
      printf("[Success]: (run time: %.3f sec)\n", run_time);
    }
    //sleep(1);

    // clean memory after parse
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();

    // quit condition
    if (context.flag_quit_) {
      printf("[Quit]: Thanks for using MiniSQL, bye!\n");
      break;
    }
  }
  delete engine;
  engine = nullptr;
  return 0;
} 