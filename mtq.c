#include <stddef.h>
#include <inttypes.h>
#include <stdatomic.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <poll.h>
#include <assert.h>
#include <pthread.h>

/*
 * head: points to first available element
 * tail: points beyond last element
 * head == tail => no content
 */

#define LENGTH(a) \
  (sizeof((a)) / sizeof((a)[0]))

static constexpr size_t k_mtqueue_max_batches_ = 8;
static constexpr size_t k_mtqueue_max_subscribers_ = 4;
static constexpr size_t k_mtqueue_num_threads_ = 4;
static constexpr size_t k_max_iters_ = 128;


typedef size_t MtQueueSubId;


typedef struct MtQueueBatch_s {
  uint8_t data[128];
  size_t  size;
} MtQueueBatch;




/*
 * Use an index pair (head;tail) to detect batches
 * taking too long to process
 */
typedef struct MtQueue_s {
  MtQueueBatch batches[k_mtqueue_max_batches_];

  pthread_cond_t have_data_cond;
  pthread_mutex_t mtx;

  struct {
    _Atomic uint32_t refmask;
  } batch_infos[k_mtqueue_max_batches_];

  struct {
    bool is_connected;
  } subs[k_mtqueue_max_subscribers_];

  _Atomic uint32_t num_subs;
  _Atomic size_t head;
  _Atomic size_t tail;
  _Atomic bool is_paused;
} MtQueue;


typedef struct ThreadArg_s {
  MtQueue *q;
  size_t thread_num;
} ThreadArg;


static inline uint32_t
build_ref_mask (uint32_t num_subs)
{
  uint32_t refmask = 0;

  for (uint32_t i = 1; i <= (1u << num_subs); i <<= 1)
    refmask |= i;

  return refmask;
}


static void
mtQueue_init (MtQueue *q)
{
  q->tail = 0;
  q->is_paused = false;

  if (pthread_mutex_init(&q->mtx, NULL) != 0) {
    perror("failed to create mutex: ");
    exit(EXIT_FAILURE);
  }

  if (pthread_cond_init(&q->have_data_cond, NULL) != 0) {
    perror("failed to create condition var: ");
    exit(EXIT_FAILURE);
  }

  memset(q->batches, 0, sizeof(q->batches));
  for (size_t i = 0; i < LENGTH(q->batch_infos); ++i)
    atomic_store(&q->batch_infos[i].refmask, 0);
}


static void
mtQueue_destroy (MtQueue *q)
{
  if (pthread_cond_destroy(&q->have_data_cond) != 0) {
    perror("failed to destroy condition var: ");
    exit(EXIT_FAILURE);
  }

  if (pthread_mutex_destroy(&q->mtx) != 0) {
    perror("failed to destroy mutex: ");
    exit(EXIT_FAILURE);
  }
}


static int
mtQueue_startBatch (MtQueue *q)
{
  auto batch = &q->batches[q->tail];
  auto batch_info = &q->batch_infos[q->tail];

  if (batch_info->refmask > 0) {
    fprintf(stderr,
     "internal error: batch %zu was not recv'd (%"PRIu32" subs left)\n",
     q->tail, batch_info->refmask);
  }

  atomic_store(&batch_info->refmask, build_ref_mask(q->num_subs));

  return 0;
}


static int
mtQueue_submitBatch (MtQueue *q)
{
  if (pthread_cond_broadcast(&q->have_data_cond) != 0) {
    perror("failed to broadcast condition: ");
    exit(EXIT_FAILURE);
  }

  _Atomic size_t old_tail = q->tail;

  printf("submitted batch %zu with %"PRIu32" refmask\n",
    old_tail, q->batch_infos[old_tail].refmask);

  atomic_store(&q->tail, (q->tail + 1) % LENGTH(q->batches));

  return 0;
}

/*
 * XXX: this subscription method is only fine
 * if we do it in advance; otherwise, race condition and sparsity
 */

static int
mtQueue_subscribe (MtQueue *q, MtQueueSubId volatile *sub_id)
{
  uint32_t nsubs = ++q->num_subs;
  *sub_id = (1 << (nsubs - 1));
  return 0;
}


static int
mtQueue_waitForBatch (MtQueue *q, MtQueueSubId sub_id, MtQueueBatch **pbatch)
{
  if (q->is_paused)
    return 1;

  while (q->head == q->tail)
    pthread_cond_wait(&q->have_data_cond, &q->mtx);

  *pbatch = &q->batches[q->head];
  auto batch_info = &q->batch_infos[q->head];

  if (atomic_fetch_and(&batch_info->refmask, ~sub_id) == sub_id)
    atomic_store(&q->head, (q->head + 1) % LENGTH(q->batches));

  printf("refmask: %"PRIu32"\n", batch_info->refmask);


  return 0;
}


static void *
thread_func (void *user_data)
{
  ThreadArg *arg = (ThreadArg *)(user_data);
  MtQueue *q = arg->q;
  printf("thread %zu started\n", arg->thread_num);

  MtQueueSubId sub_id;
  mtQueue_subscribe(q, &sub_id);
  sleep(1);

  MtQueueBatch *batch = NULL;
  int rc;
  while (!(rc = mtQueue_waitForBatch(q, sub_id, &batch))) {
    // printf("[thread %zu] got batch\n", arg->thread_num);
  }

  return NULL;
}


int
main (int argc, char *argv[argc])
{
  (void) argc;
  (void) argv;

  pthread_t threads[k_mtqueue_num_threads_];
  ThreadArg thread_args[LENGTH(threads)];
  MtQueue q;

  mtQueue_init(&q);

  for (size_t i = 0; i < LENGTH(threads); ++i) {
    thread_args[i] = (ThreadArg) {
      .q = &q,
      .thread_num = i,
    };
  }

  for (size_t i = 0; i < LENGTH(threads); ++i) {
    if (pthread_create(&threads[i],
         NULL, &thread_func, &thread_args[i]) != 0) {
      fprintf(stderr, "failed to create thread %zu\n", i);
      exit(EXIT_FAILURE);
    }
  }

  sleep(1);

  for (size_t i = 0; i < k_max_iters_; ++i) {
    mtQueue_startBatch(&q);
    printf("A: head/tail %zu/%zu\n", q.head, q.tail);
    mtQueue_submitBatch(&q);
    printf("B: head/tail %zu/%zu\n", q.head, q.tail);
    // usleep(15000);
    sleep(1);
  }
  printf(">> testing done\n");

  for (size_t i = 0; i < LENGTH(threads); ++i)
    pthread_join(threads[i], NULL);

  mtQueue_destroy(&q);

  return EXIT_SUCCESS;
}

