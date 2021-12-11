#include <fstream>
#include <algorithm>
#include <iostream>
#include <stdlib.h> // std::atoi, std::rand()
#include <iomanip>
#include <string>
#include <memory>
#include <map>
#include <chrono>
#include <filesystem>
#include "ttg.h"

#define BLOCK_SIZE 16

using namespace ttg;

template<typename T>
using Key = std::pair<std::pair<std::string, T>, T>;
template<typename T>
using MapKey = std::multimap<std::string, T>;
struct Control {};

std::mutex lock;
namespace madness {
  namespace archive {
    template <class Archive, typename T>
    struct ArchiveStoreImpl<Archive, MapKey<T>> {
      static inline void store(const Archive& ar, const MapKey<T>& mk) {
        int size = mk.size();;
        ar & size;
        typename MapKey<T>::const_iterator it = mk.begin();
        //std::cout << "Storing ..." << mk.size() << std::endl;
        //for (typename MapKey<T>::const_iterator it = mk.begin(); it != mk.end(); it++)
        while (size--) {
          ar & it->first;
          //std::cout << "Sent : " << it->first << std::endl;
          ar & it->second;
          it++;
        }
      }
    };

    template <class Archive, typename T>
    struct ArchiveLoadImpl<Archive, MapKey<T>> {
      static inline void load(const Archive& ar, MapKey<T>& mk) {
        int size;
        ar & size;
        while (size--) {
          std::string s;
          T v;
          ar & s;
          ar & v;
          //std::cout << "Loading..." << s << " " << v << std::endl;
          mk.insert(std::make_pair(s, v));
        }
      }
    };
  }
}

template <typename T>
std::ostream& operator<<(std::ostream& s, const Key<T>& key) {
  s << "Key((" << key.first.first << "," << key.first.second << "), " << key.second << ")";
  return s;
}

/*template<typename T>
auto make_reader(Edge<Key<T>, std::string>& mapEdge)
{
  auto f = [](const Key<T>& filename, std::tuple<Out<Key<T>,std::string>>& out) {
    //auto [filename, id] = key;
    //check if file exists
    std::ifstream fin(filename.first.first);
    //std::filesystem::path p{filename.first.first};

    //std::cout << "The size of " << p.u8string() << " is " << std::filesystem::file_size(p) << " bytes.\n";

    if (!fin) {
      std::cout << "File not found : " << fin << std::endl;
      ttg_abort();
    }

    //Read the file in chunks and send it to a mapper.
    std::string buffer; //reads only the first BLOCK_SIZE bytes
    buffer.resize(BLOCK_SIZE);
    int first = 0;
    int chunkID = 0;

    while(!fin.eof()) {
      char * b = const_cast< char * >( buffer.c_str() );
      fin.read(b + first, BLOCK_SIZE );
      //fin.read(buffer.data(), buffer.size());
      std::streamsize s = first + fin.gcount();
      buffer.resize(s);
      //Special handling to avoid splitting words between chunks.
      if (s > 0) {
        auto last = buffer.find_last_of(" \t\n");
        first = s - last - 1;
        std::string tmp;
        if (fin) {
          tmp.resize(BLOCK_SIZE + first);
          if (first > 0) tmp.replace(0, first, buffer, last + 1, first);
        }
        buffer.resize(last);
        //std::cout << buffer << std::endl;
        send<0>(std::make_pair(std::make_pair(filename.first.first, chunkID), 0), buffer, out);
        buffer = tmp;
        chunkID++;
      }
    }
    //This marks the end of the file and is needed for combining the output of all reducers
    //send<0>(std::make_pair(std::make_pair(filename.first.first, chunkID), 0), std::string(), out);
  };

  return make_tt<Key<T>>(f, edges(), edges(mapEdge), "reader", {}, {"mapEdge"});
  }*/

template<typename T>
void mapper(std::string chunk, MapKey<T>& resultMap) {
  //Prepare the string by removing all punctuation marks
  chunk.erase(std::remove_if(chunk.begin(), chunk.end(),
          []( auto const& c ) -> bool { return ispunct(c); } ), chunk.end());
  std::istringstream ss(chunk);
  std::string word;
  while (ss >> word)
  {
    std::transform(word.begin(), word.end(), word.begin(), ::tolower);
    //std::cout << "Mapped " << word << std::endl;
    resultMap.insert(std::make_pair(word, 1));
  }
}

template <typename T>
auto make_initiator(Edge<Key<T>, Control>& ctlEdge)
{
  auto f = [](const Key<T>& key, std::tuple<Out<Key<T>, Control>>& out) {
             send<0>(key,Control(),out);
           };
  return make_tt<Key<T>>(f, edges(), edges(ctlEdge), "initiator", {}, {"Control"});
}

template <typename funcT, typename T>
auto make_mapper(const funcT& func, Edge<Key<T>, Control>& ctlEdge,
                 Edge<Key<T>, std::string>& mapEdge,
                 Edge<Key<T>, MapKey<T>>& reduceEdge)
{
  auto f = [func](const Key<T>& key, Control& ctl, std::string& chunk,
                  std::tuple<Out<Key<T>, Control>,Out<Key<T>, MapKey<T>>>& out)
  {
    MapKey<T> resultMap;
    //If buffer is empty, Key contains all chunks for the input file, which is required for reducing reducers
    if (!chunk.empty())
    {
      //Call the mapper function
      func(chunk, resultMap);
      send<1>(key, resultMap, out);
      //Recur to get more chunks since we have not reached the end of file
      //std::cout << "P#" << ttg_default_execution_context().rank()
      //          << " processed chunk#" << key.first.second << std::endl;
      send<0>(std::make_pair(std::make_pair(key.first.first,key.first.second + 1),0), Control(), out);
    }
  };

  return make_tt(f, edges(ctlEdge, mapEdge), edges(ctlEdge, reduceEdge),
                 "mapper", {"ctlEdge","mapEdge"}, {"recur","reduceEdge"});
}

template <typename funcT, typename T>
auto make_reducer(const funcT& func, Edge<Key<T>, MapKey<T>>& reduceEdge,
                Edge<void, std::pair<std::string, T>>& writerEdge)
{
  auto f = [func](const Key<T>& key, MapKey<T>&& inputMap,
                std::tuple<Out<Key<T>, MapKey<T>>,
                Out<void, std::pair<std::string, T>>>& out)
  {
    typename MapKey<T>::iterator iter;
    int value = 0;
    //Need a tokenID to make keys unique for recurrence
    int tokenID = key.second + 1;

    iter = inputMap.begin();

    //Count of elements with same key
    int count = inputMap.count(iter->first);
    if (count > 1) {
      while (iter != inputMap.end() && !inputMap.empty())
      {
        value = func(value, iter->second);
        count--;
        if (count == 0) {
          sendv<1>(std::make_pair(iter->first, value), out);
          //std::cout << "Sent token " << tokenID << " <" << iter->first << " " << value << ">" << std::endl;
          value = 0;
        }
        inputMap.erase(iter);
        iter = inputMap.begin();
        if (count == 0) count = inputMap.count(iter->first);
      }
      if (!inputMap.empty() && iter != inputMap.end()) {
        //std::cout << "Recurring token " << tokenID << "key: " << key.first << std::endl;
        send<0>(std::make_pair(key.first, tokenID), inputMap, out);
      }
    }
    else {
      sendv<1>(std::make_pair(iter->first, iter->second), out);
      //std::cout << "Sent token " << tokenID << " <" << iter->first << " " << iter->second << ">" << std::endl;
      inputMap.erase(iter);
      if (!inputMap.empty()) {
        iter = inputMap.begin();
        if (iter != inputMap.end()) {
          //std::cout << "Recurring token " << tokenID << "key: " << key.first.second << std::endl;
          send<0>(std::make_pair(key.first, tokenID), inputMap, out);
        }
      }
    }
  };

  return make_tt(f, edges(reduceEdge), edges(reduceEdge, writerEdge), "reducer", {"reduceEdge"},
            {"recurReduceEdge","writerEdge"});
}

template<typename T>
auto make_writer(std::map<std::string, T>& resultMap, Edge<void, std::pair<std::string, T>>& writerEdge)
{
  auto f = [&resultMap](std::pair<std::string, T> &value, std::tuple<>& out) {
    //Is lock required since multiple threads can stream the tokens to this unique task.
    auto it = resultMap.find(value.first);
    if (it != resultMap.end())
      resultMap[value.first] += value.second;
    else
      resultMap.insert(value);
    //std::cout << "Write Rank : " << ttg_default_execution_context().rank() << std::endl;
    //std::cout << "Wrote: " << value.first << " " << resultMap[value.first] << std::endl;
  };

  return make_tt<void>(f, edges(writerEdge), edges(), "writer", {"writerEdge"}, {});
}

//TODO: Correct starting position needs to be checked so words are not split.
//This is the generator function used for pull edges.
//Returns the request chunk based on the input key.
const std::string get_chunk(const Key<int>& k)
{
  Key<int> key = std::any_cast<Key<int>>(k);
  //check if file exists
  std::ifstream fin(key.first.first);
  if (!fin) {
    std::cout << "File not found : " << fin << std::endl;
    ttg_abort();
  }

  //Read the file in chunks and send it to a mapper.
  std::string buffer; //reads only the first BLOCK_SIZE bytes
  int chunkID = key.first.second;
  std::cout << "P#" << ttg_default_execution_context().rank() << " requesting chunk: " << chunkID << std::endl;
  int first = chunkID * BLOCK_SIZE;
  std::filesystem::path p{key.first.first};

  size_t file_size = std::filesystem::file_size(p);
  if (file_size < first)
    return buffer;

  fin.seekg(first,std::ios::cur);

  if(!fin.eof()) {
    char * b = const_cast< char * >( buffer.c_str() );
    fin.read(b, BLOCK_SIZE);
    std::streamsize s = fin.gcount();
    buffer.resize(s);
    //Special handling to avoid splitting words between chunks.
    if (s > 0) {
      auto last = buffer.find_last_of(" \t\n");
      //auto beg = buffer.find_first_of(" \t\n");
      first = s - last - 1;
      std::string tmp;
      if (fin) {
          tmp.resize(BLOCK_SIZE + first);
          if (first > 0) tmp.replace(0, first, buffer, last + 1, first);
      }
      buffer.resize(last);

      std::cout << first << " " << (first + last) << " "
                << std::to_string(fin.tellg())
                << " " << buffer << std::endl;
    }
  }

  return buffer;
}

int main(int argc, char* argv[]) {
  if (argc < 2)
  {
    std::cout << "Usage: ./mapreduce file1 [file2, ...]\n";
    exit(-1);
  }

  std::chrono::time_point<std::chrono::high_resolution_clock> beg, end;

  ttg::ttg_initialize(argc, argv, -1);
  //OpBase::set_trace_all(true);

  //madness::redirectio(madness::World::get_default(), false);

  std::function<Key<int> (const Key<int>&)> get_index =
    [](const Key<int>& key) {
      return key;
    };

  //Let's keep all the data on Process 0 for simplicity - to focus on other functionality
  auto chunk_keymap = [](const Key<int>& key) {
                        return 0;
                      };

  Edge<Key<int>, Control> ctlEdge("control");
  Edge<Key<int>, std::string> mapEdge("mapper",true,get_chunk,get_index,chunk_keymap);
  Edge<Key<int>, MapKey<int>> reduceEdge;
  Edge<void, std::pair<std::string, int>> writerEdge;

  //auto rd = make_reader(mapEdge);
  auto init = make_initiator(ctlEdge);
  auto m = make_mapper(mapper<int>, ctlEdge, mapEdge, reduceEdge);
  auto r = make_reducer(std::plus<int>(), reduceEdge, writerEdge);

  std::map<std::string, int> result;
  auto w = make_writer(result, writerEdge);

  auto connected = make_graph_executable(init.get());
  assert(connected);
  TTGUNUSED(connected);
  //std::cout << "Graph is connected.\n";

  if (ttg_default_execution_context().rank() == 0) {
    //std::cout << "==== begin dot ====\n";
    //std::cout << Dot()(rd.get()) << std::endl;
    //std::cout << "==== end dot ====\n";

    beg = std::chrono::high_resolution_clock::now();
    for (int i = 1; i < argc; i++) {
      std::string s(argv[i]);
      //We can split the file based on number of processes, get positions.
      //Send the position to mapper.
      /*int world_size = ttg_default_execution_context().size();
        std::filesystem::path p{key.first.first};
        size_t file_size = std::filesystem::file_size(p);
        size_t fsize_per_proc = file_size / world_size;
        size_t last_proc_fsize = fsize_per_proc + (file_size % world_size);*/

      init->invoke(std::make_pair(std::make_pair(s,0),0));
    }
  }

  execute();
  fence();

  if (ttg::ttg_default_execution_context().rank() == 0) {
    end = std::chrono::high_resolution_clock::now();
    std::cout << "Mapreduce took " <<
        (std::chrono::duration_cast<std::chrono::seconds>(end - beg).count()) <<
        " seconds" << std::endl;
    std::cout << "==================== RESULT ===================\n";
    for(auto it : result) {
      std::cout << it.first << " " << it.second << std::endl;
    }
  }

  /*std::cout << "==================== RESULT from rank " << ttg_default_execution_context().rank() << " ===================\n";
    for(auto it : result) {
      std::cout << it.first << " " << it.second << std::endl;
    }*/
  ttg::ttg_finalize();
  return 0;
}
