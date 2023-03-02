#include <stdio.h>
#include "parser/syntax_tree.h"

extern int minisql_parser_line_no_;
extern int minisql_parser_column_no_;
extern int minisql_parser_debug_node_count_;
extern pSyntaxNodeList minisql_parser_syntax_node_list_;

pSyntaxNode CreateSyntaxNode(SyntaxNodeType type, char *val) {
  pSyntaxNode node = (pSyntaxNode) malloc(sizeof(struct SyntaxNode));
  node->id_ = minisql_parser_debug_node_count_++;
  node->type_ = type;
  node->line_no_ = minisql_parser_line_no_;
  node->col_no_ = minisql_parser_column_no_;
  node->child_ = NULL;
  node->next_ = NULL;
  // deep copy
  if (val != NULL) {
    // special for string, remove ""

    if (type == kNodeString) {
      size_t len = strlen(val) - 1;   // -2 + 1
      node->val_ = (char *) malloc(len);
      strncpy(node->val_, val + 1, len - 1);
      node->val_[len - 1] = '\0';
    } else {
      size_t len = strlen(val) + 1;
      node->val_ = (char *) malloc(len);
      strcpy(node->val_, val);
      node->val_[len - 1] = '\0';
    }
  } else {
    node->val_ = NULL;
  }
  // linked to syntax node list
  pSyntaxNodeList list_node = (pSyntaxNodeList) malloc(sizeof(struct SyntaxNodeList));
  list_node->node_ = node;
  if (minisql_parser_syntax_node_list_ == NULL) {
    minisql_parser_syntax_node_list_ = list_node;
    list_node->next_ = NULL;
  } else {
    list_node->next_ = minisql_parser_syntax_node_list_;
    minisql_parser_syntax_node_list_ = list_node;
  }
#ifdef ENABLE_PARSER_DEBUG
  printf("Create syntax node: node_id = %d, type = %s, line = %d, col = %d\n", node->id_,
         GetSyntaxNodeTypeStr(node->type_), node->line_no_, node->col_no_);
#endif
  return node;
}

void FreeSyntaxNode(pSyntaxNode node) {
  if (node != NULL) {
#ifdef ENABLE_PARSER_DEBUG
    printf("Free syntax node: node_id = %d\n", node->id_);
#endif
    if (node->val_ != NULL) {
      free(node->val_);
    }
    free(node);
  }
}

void CopySyntaxNode(pSyntaxNode nc, pSyntaxNode n)
{
    if(n==NULL || nc==NULL)
      return;

    nc->id_ = n->id_;
    nc->type_ = n->type_;
    nc->line_no_ = n->line_no_;
    nc->col_no_ = n->col_no_;
    //deep copy val
    if (n->val_ != NULL) {
      size_t len = strlen(n->val_) + 1;
      nc->val_ = (char *) malloc(len);
      strcpy(nc->val_, n->val_);
      nc->val_[len - 1] = '\0';
    } else {
      nc->val_ = NULL;
    }
}

void CopyRecursively(pSyntaxNode nc, pSyntaxNode n)
{
    if(n==NULL || nc==NULL)
      return;
    
    CopySyntaxNode(nc, n);

    if(n->child_!=NULL)
    {
      nc->child_ = (pSyntaxNode) malloc(sizeof(struct SyntaxNode));
      CopyRecursively(nc->child_, n->child_);

      pSyntaxNode curc = nc->child_;
      pSyntaxNode cur = n->child_;
      while(cur->next_!=NULL)
      {
        curc->next_ = (pSyntaxNode) malloc(sizeof(struct SyntaxNode));
        CopyRecursively(curc->next_, cur->next_);
        cur = cur->next_;
        curc = curc->next_;
      }
      curc->next_ = NULL;
    }
    else
    {
      nc->child_ = NULL;
    }
}

//deep copy minisql syntax tree
pSyntaxNode CopySyntaxTree(pSyntaxNode node)
{
  if(node==NULL)
    return NULL;

  pSyntaxNode ret = (pSyntaxNode) malloc(sizeof(struct SyntaxNode));
  ret->next_ = NULL;
  CopyRecursively(ret, node);
  
  return ret;
}

void FreeRecursively(pSyntaxNode n)
{
    if(n==NULL)
      return;

    if(n->child_!=NULL)
    {
      pSyntaxNode cur = n->child_;
      pSyntaxNode cur_next = cur->next_;
      FreeRecursively(cur);
      //free(cur);
      
      while(cur_next!=NULL)
      {
        cur = cur_next;
        cur_next = cur->next_;
        FreeRecursively(cur);
        //free(cur);
      }
    }

    if(n->val_!=NULL)
    {
      free(n->val_);
      n->val_ = NULL;
    }

    free(n);
}

void FreeSyntaxTree(pSyntaxNode node)
{
  FreeRecursively(node);
}

void DestroySyntaxTree() {
  pSyntaxNodeList p = minisql_parser_syntax_node_list_;
  while (p != NULL) {
    pSyntaxNodeList next = p->next_;
    FreeSyntaxNode(p->node_);
    free(p);
    p = next;
  }
  minisql_parser_syntax_node_list_ = NULL;
}

void SyntaxNodeAddChildren(pSyntaxNode parent, pSyntaxNode child) {
  if (parent->child_ == NULL) {
    parent->child_ = child;
    return;
  }
  pSyntaxNode p = parent->child_;
  while (p->next_ != NULL) {
    p = p->next_;
  }
  p->next_ = child;
}

void SyntaxNodeAddSibling(pSyntaxNode node, pSyntaxNode sib) {
  if (node == NULL || sib == NULL) {
    return;
  }
  if (node->next_ == NULL) {
    node->next_ = sib;
    return;
  }
  pSyntaxNode p = node->next_;
  while (p->next_ != NULL) {
    p = p->next_;
  }
  p->next_ = sib;
}

const char *GetSyntaxNodeTypeStr(SyntaxNodeType type) {
  switch (type) {
    case kNodeUnknown:
      return "kNodeUnknown";
    case kNodeQuit:
      return "kNodeQuit";
    case kNodeExecFile:
      return "kNodeExecFile";
    case kNodeIdentifier:
      return "kNodeIdentifier";
    case kNodeNumber:
      return "kNodeNumber";
    case kNodeString:
      return "kNodeString";
    case kNodeNull:
      return "kNodeNull";
    case kNodeCreateDB:
      return "kNodeCreateDB";
    case kNodeDropDB:
      return "kNodeDropDB";
    case kNodeShowDB:
      return "kNodeShowDB";
    case kNodeUseDB:
      return "kNodeUseDB";
    case kNodeShowTables:
      return "kNodeShowTables";
    case kNodeCreateTable:
      return "kNodeCreateTable";
    case kNodeDropTable:
      return "kNodeDropTable";
    case kNodeShowIndexes:
      return "kNodeShowIndexes";
    case kNodeInsert:
      return "kNodeInsert";
    case kNodeDelete:
      return "kNodeDelete";
    case kNodeUpdate:
      return "kNodeUpdate";
    case kNodeSelect:
      return "kNodeSelect";
    case kNodeConditions:
      return "kNodeConditions";
    case kNodeConnector:
      return "kNodeConnector";
    case kNodeCompareOperator:
      return "kNodeCompareOperator";
    case kNodeColumnType:
      return "kNodeColumnType";
    case kNodeColumnDefinition:
      return "kNodeColumnDefinition";
    case kNodeColumnDefinitionList:
      return "kNodeColumnDefinitionList";
    case kNodeColumnList:
      return "kNodeColumnList";
    case kNodeColumnValues:
      return "kNodeColumnValues";
    case kNodeUpdateValues:
      return "kNodeUpdateValues";
    case kNodeUpdateValue:
      return "kNodeUpdateValue";
    case kNodeAllColumns:
      return "kNodeAllColumns";
    case kNodeCreateIndex:
      return "kNodeCreateIndex";
    case kNodeDropIndex:
      return "kNodeDropIndex";
    case kNodeTrxBegin:
      return "kNodeTrxBegin";
    case kNodeTrxCommit:
      return "kNodeTrxCommit";
    case kNodeTrxRollback:
      return "kNodeTrxRollback";
    default:
      return "error type";
  }
}
