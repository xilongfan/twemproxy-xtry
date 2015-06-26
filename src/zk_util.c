#include "./zk_util.h"
#include <time.h>

#define UNUSED

extern int get_node_data(zookeeper_client * zkc_ptr, char * node_path, char ** data) {
  int ret_code = -1;
  if (NULL == zkc_ptr || NULL == zkc_ptr->zk_ptr ||
      NULL == node_path || NULL == data) { return ret_code; }
  char data_buf[MAX_ELECTION_NODE_DATA_LEN];
  memset(data_buf, 0, sizeof(data_buf));
  int buf_len = sizeof(data_buf);
  int ret_val = zoo_get(zkc_ptr->zk_ptr, node_path, 0, data_buf, &buf_len, NULL);
  if (ZOK == ret_val) {
    allocate_and_copy_str(data, data_buf);
    ret_code = 0;
  }
  return ret_code;
}

zookeeper_client * create_zookeeper_client()
{
  zookeeper_client * zkc_ptr = (zookeeper_client * )malloc(sizeof(zookeeper_client));
  if (NULL == zkc_ptr) { return zkc_ptr; }
  zkc_ptr->zk_ptr = NULL;
  zkc_ptr->root_path = NULL;
  zkc_ptr->zk_conn_str = NULL;
  zkc_ptr->max_zkc_timeout = MAX_SESSION_TIME_OUT;
  zkc_ptr->callback_ret_val = CALLBACK_RET_INVALID_VAL;
  pthread_mutex_init(&(zkc_ptr->zk_mutex), NULL);
  pthread_cond_init(&(zkc_ptr->cond_var), NULL);

  return zkc_ptr;
}
int init_zkc_connection(zookeeper_client * zkc_ptr,
                        const char * conn_str, const char * root_str) {
  return init_zkc_connection_tm(zkc_ptr, conn_str, root_str, MAX_SESSION_TIME_OUT);
}
 
int init_zkc_connection_tm(zookeeper_client * zkc_ptr,
                           const char * conn_str, const char * root_str,
                           int max_zkto) {
  int ret_code = -1;
  if ((NULL == conn_str) || (NULL == root_str)){ return ret_code; }
  int conn_str_len = strlen(conn_str), root_str_len = strlen(root_str);
  if ((0 == conn_str_len) || (MAX_STR_VAL_SIZE < conn_str_len) ||
      (0 == root_str_len) || (MAX_STR_VAL_SIZE < root_str_len) ||
      (NULL == zkc_ptr)  || (0 > max_zkto)) { return ret_code; }

  /** automatically blocks until a session has been set up successfully. */
  pthread_mutex_lock(&(zkc_ptr->zk_mutex));

  zkc_ptr->max_zkc_timeout = max_zkto;

  zkc_ptr->zk_conn_str = (char * )malloc(conn_str_len + 1);
  assert(NULL != zkc_ptr->zk_conn_str);
  memset(zkc_ptr->zk_conn_str, 0, conn_str_len + 1);
  strncpy(zkc_ptr->zk_conn_str, conn_str, conn_str_len);

  zkc_ptr->root_path = (char * )malloc(conn_str_len + 1);
  assert(NULL != zkc_ptr->root_path);
  memset(zkc_ptr->root_path, 0, root_str_len + 1);
  strncpy(zkc_ptr->root_path, root_str, root_str_len);
  
  zkc_ptr->zk_ptr = zookeeper_init(
    zkc_ptr->zk_conn_str, zk_init_callback, zkc_ptr->max_zkc_timeout, 0, zkc_ptr, 0
  );

  if (NULL == zkc_ptr->zk_ptr) { return ret_code; }

  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  ts.tv_sec += MAX_COND_WAIT_TIME_OUT;
  int rc = 0;
  while (CALLBACK_RET_INVALID_VAL == zkc_ptr->callback_ret_val && rc == 0) {
    rc = pthread_cond_timedwait(&(zkc_ptr->cond_var), &(zkc_ptr->zk_mutex), &ts);
  }
  if (ZOO_CONNECTED_STATE == zkc_ptr->callback_ret_val) { ret_code = 0; }
  zkc_ptr->callback_ret_val = CALLBACK_RET_INVALID_VAL;

  pthread_mutex_unlock(&(zkc_ptr->zk_mutex));

  return ret_code;
}

int free_zookeeper_client(zookeeper_client * zkc_ptr) {
  /** automatically blocks until a session has been set up successfully. */
  free(zkc_ptr->zk_conn_str);
  free(zkc_ptr->root_path);
  if (NULL != zkc_ptr->zk_ptr) { zookeeper_close(zkc_ptr->zk_ptr); }
  pthread_mutex_destroy(&(zkc_ptr->zk_mutex));
  pthread_cond_destroy(&(zkc_ptr->cond_var));
  free(zkc_ptr);
  return 0;
}

int batch_delete_atomic(zookeeper_client * zkc_ptr,
                        char ** nodes_todel, int nodes_cnt)
{
  int ret_code = -1;

  if (NULL == zkc_ptr || NULL == zkc_ptr->zk_ptr) { return ret_code; }

  int tot_size = nodes_cnt;
  int itr_cnts = tot_size / MAX_BATCH_CHUNK;
  int tsk_remn = tot_size - itr_cnts * MAX_BATCH_CHUNK;
  if (tsk_remn > 0) { itr_cnts++; }
 
  int prev = 0;
  int next = 0;

  for (int c = 0; c < itr_cnts; c++)
  {
    next += MAX_BATCH_CHUNK;
    next = (next > tot_size) ? tot_size : next;

    int node_cnt = next - prev;
    char ** revr_nodes_todel = (char **)malloc(sizeof(char *) * (node_cnt));
    for (int i = prev; i < next; i++) { revr_nodes_todel[i - prev] = nodes_todel[i]; }
  
    zoo_op_result_t * ret_set = (zoo_op_result_t *)malloc(sizeof(zoo_op_result_t) * node_cnt);
    zoo_op_t *        ops_set = (zoo_op_t *)malloc(sizeof(zoo_op_t) * node_cnt);
    assert(NULL != ops_set);
    assert(NULL != ret_set);
    memset(ret_set, 0, sizeof(zoo_op_result_t) * node_cnt);
    memset(ops_set, 0, sizeof(zoo_op_t) * node_cnt);
  
    for (int i = 0; i < node_cnt; i++) {
      zoo_delete_op_init(&ops_set[i], revr_nodes_todel[i], -1);
    } /** end of for **/
    int rval = zoo_multi(zkc_ptr->zk_ptr, node_cnt, ops_set, ret_set);
    if ((int)ZOK == rval) {
fprintf(stderr, "del: %s", revr_nodes_todel[0]);
      ret_code = 0;
    }

    free(ret_set);
    free(ops_set);
    free(revr_nodes_todel);

    prev = next;
  } /** end of for **/
  return ret_code;
}

int batch_create_atomic(zookeeper_client * zkc_ptr, char ** path_arr,
                        char ** data_arr, int path_cnt)
{
  int ret_code = -1;

  if (NULL == zkc_ptr || NULL == zkc_ptr->zk_ptr) { return ret_code; }

  int tot_size = path_cnt;
  int itr_cnts = tot_size / MAX_BATCH_CHUNK;
  int tsk_remn = tot_size - itr_cnts * MAX_BATCH_CHUNK;
  if (tsk_remn > 0) { itr_cnts++; }
 
  int prev = 0;
  int next = 0;

  for (int c = 0; c < itr_cnts; c++)
  {
    next += MAX_BATCH_CHUNK;
    next = (next > tot_size) ? tot_size : next;

    int node_cnt = next - prev;

    char ** batch_path_arr = (char **)malloc(sizeof(char *) * (node_cnt));
    char ** batch_data_arr = (char **)malloc(sizeof(char *) * (node_cnt));
    for (int i = prev; i < next; i++) {
      batch_path_arr[i - prev] = path_arr[i];
      batch_data_arr[i - prev] = data_arr[i];
    }
  
    zoo_op_result_t * ret_set = (zoo_op_result_t *)malloc(sizeof(zoo_op_result_t) * node_cnt);
    zoo_op_t *        ops_set = (zoo_op_t *)malloc(sizeof(zoo_op_t) * node_cnt);
    assert(NULL != ops_set);
    assert(NULL != ret_set);
    memset(ret_set, 0, sizeof(zoo_op_result_t) * node_cnt);
    memset(ops_set, 0, sizeof(zoo_op_t) * node_cnt);
  
    char ret_path[MAX_PATH_LEN];
    for (int i = 0; i < node_cnt; i++) {
      zoo_create_op_init(
        &ops_set[i], batch_path_arr[i], batch_data_arr[i], strlen(data_arr[i]),
        &ZOO_OPEN_ACL_UNSAFE, 0, ret_path, sizeof(ret_path)
      );
    } /** end of for **/
    int rval = zoo_multi(zkc_ptr->zk_ptr, node_cnt, ops_set, ret_set);
    if ((int)ZOK == rval) { ret_code = 0; }
    free(ret_set);
    free(ops_set);
    prev = next;
  } /** end of for **/
  return ret_code;
}

int allocate_and_copy_str_tm(char ** dest, char * src, int limit)
{
  int src_len = strlen(src);
  if (NULL == dest || NULL == src || limit < src_len) { return -1; }
  * dest = (char * )malloc(src_len + 1);
  assert(NULL != * dest);
  memset(* dest, 0, src_len + 1);
  strncpy(* dest, src, src_len);
  return 0;
} 

int allocate_and_copy_str(char ** dest, char * src) {
  return allocate_and_copy_str_tm(dest, src, MAX_STR_VAL_SIZE);
}

int get_child_nodes(zookeeper_client * zkc_ptr, char * dir_path,
                    char *** node_arr_ptr, int * node_cnt_ptr) {
  int ret_code = -1;
  assert(NULL != zkc_ptr && NULL != zkc_ptr->zk_ptr && NULL != node_cnt_ptr &&
         NULL != node_cnt_ptr && NULL != dir_path);
  if (0 >= strlen(dir_path)) { return ret_code; }
  struct String_vector child_nodes_arr;
  child_nodes_arr.data = NULL;
  child_nodes_arr.count = 0;
  int ret_val = zoo_get_children(zkc_ptr->zk_ptr, dir_path, 0, &child_nodes_arr);
  if (ZOK == ret_val) {
    sort_child_nodes_arr(&child_nodes_arr);
    * node_cnt_ptr = child_nodes_arr.count;
    if (* node_cnt_ptr > 0) {
      * node_arr_ptr = (char **)malloc(sizeof(char *) * (* node_cnt_ptr));
      for (int i = 0; i < child_nodes_arr.count; i++) {
        allocate_and_copy_str(&((*node_arr_ptr)[i]), child_nodes_arr.data[i]);
      }
    }
    ret_code = 0;
  }
  deallocate_String_vector(&child_nodes_arr);
  return ret_code;
}

int create_hb_node(zookeeper_client * zkc_ptr, const char * root_path,
                   const char * data_str, char ** path_created) {
  int ret_code = 0;

  assert(NULL != zkc_ptr && NULL != zkc_ptr->zk_ptr && NULL != root_path &&
         NULL != data_str && NULL != path_created);

  char path_str[MAX_PATH_LEN];
  memset(path_str, 0, sizeof(path_str));
  sprintf(path_str, "%s%s", root_path, DEF_INSTANCE_HB_PREFIX);

  * path_created = (char*)malloc(sizeof(char) * MAX_PATH_LEN);
  memset(* path_created, 0, MAX_PATH_LEN);

  int ret_val = zoo_create(
    zkc_ptr->zk_ptr, path_str, data_str, strlen(data_str),
    &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL | ZOO_SEQUENCE,
    * path_created, MAX_PATH_LEN
  ); 
  if (ZOK == ret_val) { ret_code = 0; }
  return ret_code;
}

int vstrcmp(const void* str1, const void* str2) {
    const char **a = (const char**) str1;
    const char **b = (const char**) str2;
    // return strcmp(strrchr(* a, '-') + 1, strrchr(* b, '-') + 1); 
    return strcmp(* a, * b); 
} 

void sort_child_nodes_arr(
    struct String_vector * child_nodes_arr)
{
    qsort(
      child_nodes_arr->data, child_nodes_arr->count, sizeof(char*), &vstrcmp
    );
}

char * get_ip_addr_v4_lan() {
  char * local_ip = NULL;
  struct ifconf ifconf;
  struct ifreq ifr[MAX_ELECTION_NODE_INTERFACES_CNT];
  int fd_sock = -1;
  int if_cnt = 0;
  int ret_val = -1;

  fd_sock = socket(AF_INET, SOCK_STREAM, 0);

  assert(0 < fd_sock);
  ifconf.ifc_buf = (char *) ifr;
  ifconf.ifc_len = sizeof(ifr);
  ret_val = ioctl(fd_sock, SIOCGIFCONF, &ifconf);

  close(fd_sock);

  assert (0 == ret_val);
  if_cnt = ifconf.ifc_len / sizeof(ifr[0]);
  for (int i = 0; i < if_cnt; i++) {
    char ip[INET_ADDRSTRLEN + 1] = { 0 };
    struct sockaddr_in * s_in = (struct sockaddr_in *) &ifr[i].ifr_addr;
    assert(NULL != inet_ntop(AF_INET, &s_in->sin_addr, ip, sizeof(ip)));
    if (0 == strcmp(ELECTION_NODE_INV_IP_V4, ip)) {
      continue;
    } else {
      allocate_and_copy_str(&local_ip, ip);
      break;
    }
  }
fprintf (stderr, "==>> %s", local_ip);
  return local_ip;
}

void zk_init_callback(zhandle_t * zkc, int type, int state,
                      const char * path, void * zk_proxy_ptr) {
  zookeeper_client * zkc_ptr = ((zookeeper_client *)zk_proxy_ptr);
  /**
   * here we use mutex to make sure the main thread calling init_zk_conn is
   * waiting for the cond_var before we sending the signal, since we
   * should never depend on sleep to serialize threads.
   */
  pthread_mutex_lock(&(zkc_ptr->zk_mutex));
  zkc_ptr->callback_ret_val = state;
  pthread_cond_signal(&(zkc_ptr->cond_var));
  pthread_mutex_unlock(&(zkc_ptr->zk_mutex));
}
