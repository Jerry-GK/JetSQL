#include <cstdio>
#include <time.h>
#include <signal.h>
#include "executor/execute_engine.h"
#include "glog/logging.h"
#include "parser/syntax_tree_printer.h"
#include "utils/tree_file_mgr.h"

extern "C" {
int yyparse(void);
FILE *yyin;
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

ExecuteEngine *engine = nullptr;

void quit_flush(int sig_num)
{
  cout<<"\n[Exception]: Forced quit!Signal number :" << sig_num <<endl;
  delete engine;//deconstruction will flush all dirty pages in buffer pool back to disk 
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
    input[i++] = ch;
  }
  input[i] = ch;    // ;
  getchar();        // remove enter
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

  while (1) {
    // read from buffer
    InputCommand(cmd, buf_size);
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

    ExecuteContext context;
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