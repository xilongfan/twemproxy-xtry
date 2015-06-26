#ifndef ZK_UTIL_H_
#define ZK_UTIL_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zookeeper/zookeeper.h>
#include <zookeeper/zookeeper.jute.h>
#include <assert.h>
#include <time.h>
#include <errno.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <linux/netdevice.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <fcntl.h>

static const int MAX_ELECTION_NODE_INTERFACES_CNT = 16;
static const int MAX_ELECTION_NODE_DATA_LEN = 1023;
static const int MAX_SESSION_TIME_OUT = 50000;
static const int MAX_COND_WAIT_TIME_OUT = 40;
static const int CALLBACK_RET_INVALID_VAL = -1;
static const int MAX_STR_VAL_SIZE = 1023;
static const int MAX_BATCH_CHUNK = 512;
static const int MAX_PATH_LEN = 512;
static char * const ELECTION_NODE_INV_IP_V4 = "127.0.0.1";
static char * const DEF_INSTANCE_HB_PREFIX = "/redis-inst/redis-";

typedef struct zookeeper_client_st {
  zhandle_t * zk_ptr;
  char * zk_conn_str;
  char * root_path;

  pthread_cond_t cond_var;
  pthread_mutex_t zk_mutex;
  int max_zkc_timeout;
  int callback_ret_val;
} zookeeper_client;

extern zookeeper_client * create_zookeeper_client();

extern int init_zkc_connection_tm(zookeeper_client * zkc_ptr,
                                  const char * conn_str,
                                  const char * root_str, int max_zkto);
extern int init_zkc_connection(zookeeper_client * zkc_ptr,
                               const char * conn_str,
                               const char * root_str);

extern int free_zookeeper_client(zookeeper_client * zkc_ptr);
extern int batch_delete_atomic(zookeeper_client * zkc_ptr,
                               char ** path_arr, int path_cnt);
extern int batch_create_atomic(zookeeper_client * zkc_ptr,
                               char ** path_arr, char ** data_arr,int path_cnt);

extern int get_node_data(zookeeper_client * zkc_ptr, char * node_path, char ** data);

extern int get_child_nodes(zookeeper_client * zkc_ptr,
                           char * dir_path, char *** node_arr, int * node_cnt);

extern int create_hb_node(zookeeper_client * zkc_ptr,
                          const char * path_str, const char * data_str,
                          char ** path_created);

int allocate_and_copy_str_tm(char ** dest, char * src, int limit);
int allocate_and_copy_str(char ** dest, char * src);

int vstrcmp(const void * l_str, const void * r_str);
void sort_child_nodes_arr(struct String_vector * child_nodes_arr);
char * get_ip_addr_v4_lan();
void zk_init_callback(zhandle_t *, int type, int state,
                      const char * path, void * zk_proxy_ptr);

#endif
