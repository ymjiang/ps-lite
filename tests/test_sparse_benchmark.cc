#include <chrono>
#include <cmath>
#include <cstdlib>
#include <unistd.h>
#include "ps/ps.h"

#define DIVUP(x, y) (((x)+(y)-1)/(y))
#define ROUNDUP(x, y) (DIVUP((x), (y))*(y))
#define DEBUG_PRINT_TENSOR_VALUE(X) (*((float *)(X) + 0))
#define DEBUG_PRINT_TENSOR_ADDRESS(X) (reinterpret_cast<uint64_t>(X))

using namespace ps;

enum MODE {
    PUSH_THEN_PULL = 0,
    PUSH_PULL = 1,
    PUSH_ONLY = 2, 
    PULL_ONLY = 3
};
std::unordered_map<uint64_t, KVPairs<char> > mem_map_push;
std::unordered_map<uint64_t, KVPairs<char> > mem_map_pull;
bool debug_mode_ = false;

void aligned_memory_alloc(void** ptr, size_t size) {
  size_t page_size = sysconf(_SC_PAGESIZE);
  void* p;
  int size_aligned = ROUNDUP(size, page_size);
  int ret = posix_memalign(&p, page_size, size_aligned);
  CHECK_EQ(ret, 0) << "posix_memalign error: " << strerror(ret);
  CHECK(p);
  memset(p, 1, size);
  *ptr = p;
}

void float_sum(float *dst, float *src, size_t len) {
  if (len == 0) return;
  for (size_t i = 0; i < len / (size_t) sizeof(float); ++i) {
    dst[i] = dst[i] + src[i];
  }
}
uint64_t DecodeKey(ps::Key key) {
  auto kr = ps::Postoffice::Get()->GetServerKeyRanges()[ps::MyRank()];
  return key - kr.begin();
}

void MallocAligned(void** ptr, size_t size) {
  size_t page_size = sysconf(_SC_PAGESIZE);
  void* p;
  int size_aligned = ROUNDUP(size, page_size);
  int ret = posix_memalign(&p, page_size, size_aligned);
  CHECK_EQ(ret, 0) 
      << "posix_memalign error: " << strerror(ret);
  CHECK(p);
  memset(p, 0, size);
  *ptr = p;
}

template <typename T>
void AllocMemoryAndCreateSarray(ps::SArray<T>& sarr, T* addr, int count) {
  void* ptr;
  MallocAligned(&ptr, count * sizeof(T));
  memcpy(ptr, (void*)addr, count * sizeof(T));
  sarr.reset((T*)ptr, count, [](void *){});
}

static std::unordered_map<uint64_t, KVPairs<char>> _init_bufferLengths;
template <typename Val>
void EmptyHandler(const ps::KVMeta &req_meta, 
                         const ps::KVPairs<Val> &req_data, 
                         ps::KVServer<Val> *server) {
  uint64_t key = req_data.keys[0];
  if (req_meta.push) {
    CHECK(req_data.lens.size());
    CHECK_EQ(req_data.vals.size(), (size_t)req_data.lens[0]) 
        << "key=" << key << ", " 
        << req_data.vals.size() << ", " 
        << req_data.lens[0];

    auto recved = reinterpret_cast<char*>(req_data.vals.data());

    int len = (int) req_data.vals.size();
    if (_init_bufferLengths.find(key) == _init_bufferLengths.end()) {
      AllocMemoryAndCreateSarray(_init_bufferLengths[key].keys, (ps::Key*)&key, 1);
      AllocMemoryAndCreateSarray(_init_bufferLengths[key].vals, recved, len);
      AllocMemoryAndCreateSarray(_init_bufferLengths[key].lens, (int*)&len, 1);
    }

    LOG(INFO) << "receive push key=" << key << "\t" 
        << "len=" << len << "\t"
        << ((size_t*)recved)[0] << " from sender=" << req_meta.sender << "\n";
    
    // send push response (empty payload)
    ps::KVPairs<char> res;
    server->Response(req_meta, res);
  } else { // pull 
    LOG(INFO) << "receive pull key=" << key << " from sender=" << req_meta.sender;
    CHECK(_init_bufferLengths.find(key) != _init_bufferLengths.end()) << key;
    server->Response(req_meta, _init_bufferLengths[key]);

  }
}

void StartServer() {
  if (!IsServer()) return;
  debug_mode_ = Environment::Get()->find("DEBUG_MODE") ? true : false;

  auto server = new KVServer<char>(0);
  server->set_request_handle(EmptyHandler<char>);
  RegisterExitCallback([server]() { delete server; });
}

void CreateSarrayVector(std::vector<SArray<char> > &vec, const int len, const int key_num) {
  for (int key = 0; key < key_num; key++) {
    void* ptr;
    aligned_memory_alloc(&ptr, len);
    SArray<char> vals;
    vals.reset((char*) ptr, len * sizeof(char), [](void *){});
    vec.push_back(vals);
  }
}

using data_t = uint64_t;

void RunWorker(int argc, char *argv[]) {
  if (!IsWorker()) return;
  KVWorker<char> kv(0, 0);
  auto krs = ps::Postoffice::Get()->GetServerKeyRanges();

  const int num_servers = krs.size();
  LOG(INFO) << num_servers << " servers in total";
  CHECK_GT(num_servers, 0);
  const int len = 8;

  std::vector<ps::SArray<ps::Key>> tmpKeys;
  std::vector<ps::SArray<int>> tmpLens;
  std::vector<ps::SArray<char>> bufferLenSarrays;
  auto ps = &kv;

  int workerID = ps::MyRank() == 0 ? 0 : 1;
  std::vector<size_t> _globalTotalEmbedBufLens = {0, 0};
  if (ps::MyRank() == 0) _globalTotalEmbedBufLens[0] = 100;
  if (ps::MyRank() == 1) _globalTotalEmbedBufLens[1] = 200;
  LOG(INFO) << "My rank is " << MyRank();
  LOG(INFO) << "Before comm: (" << _globalTotalEmbedBufLens[0] 
      << ", " << _globalTotalEmbedBufLens[1] << ")";

  for (int i = 0; i < 2; i++) {
    ps::Key key = i;
    int server = i;

    // vals
    ps::SArray<char> tmp;
    tmp.reset((char*)&_globalTotalEmbedBufLens[i], sizeof(size_t), [](void *){});
    bufferLenSarrays.push_back(tmp);
    
    // keys
    // ps::Key ps_key = krs[0].begin() + key;
    // ps::SArray<ps::Key> keys;
    // keys.reset(&ps_key, 1, [](void *){});
    // tmpKeys.push_back(keys);
    void* ptr_key;
    aligned_memory_alloc(&ptr_key, sizeof(Key));
    SArray<Key> keys;
    keys.reset((Key*) ptr_key, 1, [](void *){});
    ps::Key ps_key = krs[server].begin() + key;
    memcpy(ptr_key, &ps_key, sizeof(Key));
    tmpKeys.push_back(keys);
    
    // lens
    // int ps_len = sizeof(size_t);
    // ps::SArray<int> lens;
    // lens.reset(&ps_len, 1, [](void *){});
    // tmpLens.push_back(lens);
    void* ptr_len;
    aligned_memory_alloc(&ptr_len, sizeof(int));
    SArray<int> lens;
    lens.reset((int*) ptr_len, 1, [](void *){});
    memcpy(ptr_len, &len, sizeof(len));
    tmpLens.push_back(lens);
  }

  // Push once to the associated server
  {
    int server = workerID;
    auto keys = tmpKeys[server];
    auto vals = bufferLenSarrays[server];
    auto lens = tmpLens[server];
    ps->Wait(ps->ZPush(keys, vals, lens));
  }
  std::this_thread::sleep_for(std::chrono::seconds(5));

  // Pull the embedding buffer length of other workers
  {
    int server = workerID == 0 ? 1 : 0;
    // int server = workerID;
    auto keys = tmpKeys[server];
    auto vals = bufferLenSarrays[server];
    auto lens = tmpLens[server];
    ps->Wait(ps->ZPull(keys, &vals, &lens));
  }

  LOG(INFO) << "After comm: (" << _globalTotalEmbedBufLens[0] 
      << ", " << _globalTotalEmbedBufLens[1] << ")";
}

int main(int argc, char *argv[]) {
  // disable multi-threaded processing first
  setenv("ENABLE_SERVER_MULTIPULL", "0", 1);
  // start system
  Start(0);
  // setup server nodes
  StartServer();
  // run worker nodes
  RunWorker(argc, argv);
  // stop system
  Finalize(0, true);
  return 0;
}